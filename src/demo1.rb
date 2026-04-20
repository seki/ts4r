require 'drb/drb'
require 'rinda/tuplespace'
require 'unimidi'
require 'monitor'
require_relative '../../midim/src/midim'
require_relative '../../midim/src/dj2go2'

$drum = 39
$drum = 52

class NQControl
  include MonitorMixin
  def initialize(dj2, ts)
    super()
    @dj2 = dj2
    @ts = ts
    @ractor = (3..10).to_a
    @note_on = [148, 149].product([1,2,3,4]).zip(@ractor).to_h
    @note = @note_on.to_a.map {|k, v| [v, k]}.to_h
    @ractor.each {|n| stop(n)}
    run(@ractor.first)
  end

  def sync_puts(tuple)
    synchronize do
      @dj2.output.puts(tuple)
    end
  end

  def note_on(tuple)
    pp [:on, tuple]
    index = @note_on[tuple[0..1]]
    return unless index
    if (@ts.take([:run, index], 0) rescue nil)
      stop(index)
    else
      run(index)
    end
  end

  def led(k, v)
    tuple = @note[k]
    return unless tuple
    pp (tuple + [v])
    sync_puts(tuple + [v])
  end

  def run(n)
    @ts.take([:run, n], 0) rescue nil
    @ts.write([:run, n])
    led(n, 2)
  end

  def stop(n)
    @ts.take([:run, n], 0) rescue nil
    led(n, 0)
  end
end

output = UniMIDI::Output.find_by_name("Apple Inc. IACドライバ")

ts = Rinda::TupleSpace.new
DRb.start_service('druby://localhost:8470', ts)

_, nq = ts.take([:join, nil])

bp = nq.watch_break(nil)
pp bp
nq.continue(bp.first)

dj2go2 = MidiM::DJ2GO2Dev.new
dj2go2.query_message
NTHREAD = 8

nq_control = NQControl.new(dj2go2, ts)

scale = [1,3,4,5,6,7,8,10,11,12,13].map {|x| x + 59}
scale = [60, 64, 67, 71, 74, 77, 81, 84].map {|x| x - 0}
# scale = [37, 56, 38, 46, 36, 48, 49, 50] # drum
logger = Thread.new(nq, output) do |nqueen, out|
  while true
    bp, info = nqueen.log_take
    name = bp.first
    note = scale[(name - 3) % scale.size]
    case info
    in [:nq_begin, Integer, Integer]
      # pp [name, 144, note, 120]
      pp out.puts [144, note, 100]
    in [:nq_end, Integer, Integer]
      # pp [name, 144, note, 0]
      pp out.puts [144, note, 0]
    in [:done]
      pp :done
      pp out.puts [145, 57, 120]
    else
      pp [:else, info]
      # out.puts [145, 52, 0]
      # out.puts [145, 46, 0]
      if info.last == 0
        out.puts [145, 52, 80]
        out.puts [145, 52, 0]
      else
        out.puts [145, 46, 70]
        out.puts [145, 46, 0]
      end
    end
  end
end

control = Thread.new(dj2go2) do |dj2|
  dj2.reader {|x|
    pp x
    case x[:data]
    in [148..149, 1..4 => btn, Integer]
      nq_control.note_on(x[:data])
    in [191, 8, Integer => drum]
      $drum = drum
    else
      pp x[:data]
    end
  }
end

while true
  Thread.new(ts, nq, nq.watch_break(nil)) do |ts, nq, bp_info|
    pp bp_info
    pp [:will_take, [:run, bp_info.first.first]]
    ts.read([:run, bp_info.first.first])
    nq.continue(bp_info.first)
  end
end

control.join
# logger.join

