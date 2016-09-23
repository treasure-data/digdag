require 'time'

class MyClass
  def print_time(label:, time:)
    p [label, Time.parse(time)]
  end

  def say_something(order:, animal:)
    puts %Q|#{order} #{animal} said "My favorite number is #{Random.rand(10)}"|
  end
end
