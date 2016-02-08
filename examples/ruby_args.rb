class Test
  def initialize(config)
    puts "initialized"
  end

  def task1(config)
    puts "running task1"
    puts "arg1: #{config['arg1']}"
  end

  def self.task2(config)
    puts "running task2"
    puts "arg2: #{config['arg2']}"
  end
end
