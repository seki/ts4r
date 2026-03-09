require 'rinda/tuplespace'

class TupleSpace4Ractor
  class Impl
    def initialize
      @ts = Rinda::TupleSpace.new #FIXME
      @read_waiter = []
      @take_waiter = []
    end

    def do_take(port, pattern)
      tuple = @ts.take(pattern, 0)
      port << tuple
      true
    rescue Rinda::RequestExpiredError
      false
    end

    def do_read(port, pattern)
      tuple = @ts.read(pattern, 0)
      port << tuple
      true
    rescue Rinda::RequestExpiredError
      false
    end

    def do_write(tuple)
      @ts.write(tuple)
      @read_waiter.delete_if {|port, pattern| do_read(port, pattern)}
      taken = false
      @take_waiter.delete_if {|port, pattern| 
        break if taken
        taken = do_take(port, pattern)
      }
    end

    def main_loop
      while true
        command, tuple, port = Ractor.receive
        case command
        when :read
          @read_waiter << [port, tuple] unless do_read(port, tuple)
        when :take
          @take_waiter << [port, tuple] unless do_take(port, tuple)
        when :write
          do_write(tuple)
        end
      end
    end
  end

  def initialize
    @ractor = Ractor.new { Impl.new.main_loop }
  end

  def take(pattern)
    port = Ractor::Port.new
    @ractor << [:take, pattern, port]
    port.receive
  end

  def read(pattern)
    port = Ractor::Port.new
    @ractor << [:read, pattern, port]
    port.receive
  end

  def write(tuple)
    @ractor << [:write, tuple]
    tuple
  end
end

