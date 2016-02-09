class Test
  def task1(arg1:)
    puts "running task1"
    puts "arg1: #{arg1}"
  end

  def self.task2(arg2:, arg3: "this is default value")
    puts "running task2"
    puts "arg2: #{arg2}"
    puts "arg3: #{arg3}"
  end
end
