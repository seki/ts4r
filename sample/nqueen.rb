require_relative '../src/ts'

module NQueen
  module_function
  def concat(board, row)
    board.each_with_index do |v, col|
      check = (v - row).abs
      return nil if check == 0
      return nil if check == board.size - col
    end
    board + [row]
  end

  def nq(size, board=[])
    found = 0
    size.times do |row|
      fwd = concat(board, row)
      next unless fwd
      return 1 if fwd.size == size
      found += nq(size, fwd)
    end
    found
  end

  def nq2(size, r1, r2)
    board = concat([r1], r2)
    return 0 unless board
    nq(size, board)
  end
end

def invoke_engine(rinda, num)
  num.times do
    Ractor.new(rinda) do |ts|
      while true
        ts.break('nq_loop')
        sym, size, r1, r2 = ts.take([:nq, Integer, Integer, Integer])
        ts.log([:nq_begin, r1, r2])
        ts.write([:nq_ans, size, r1, r2, NQueen.nq2(size, r1, r2)])
        ts.log([:nq_end, r1, r2])
      end
    end
  end
end

def write_q(rinda, size)
  size.times do |r1|
    size.times do |r2|
      rinda.write([:nq, size, r1, r2])
    end
  end
end

def take_a(rinda, size)
  found = 0
  size.times.reverse_each do |r1|
    size.times.reverse_each do |r2|
      tuple = rinda.take([:nq_ans, size, r1, r2, nil])
      rinda.log(tuple)
      found += tuple[4]
    end
  end
  found
end

def resolve(rinda, size)
  write_q(rinda, size)
  take_a(rinda, size)
end

rinda = TupleSpace4Ractor::Aether
size = (ARGV.shift || '14').to_i

DRb.start_service('druby://localhost:0', rinda)
shell = DRbObject.new_with_uri('druby://localhost:8470')
shell.write([:join, rinda])

pp rinda.break('stop')

invoke_engine(rinda, 8)
puts resolve(rinda, size)
rinda.log([:done])

# puts resolve(rinda, size)

pp rinda.break('done')

exit!