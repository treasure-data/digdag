class InstanceVariable
  def initialize(arg1)
    @var = arg1
  end

  def step1
    puts @var
    puts Digdag.env.params['arg1']
    puts Digdag.env.params['arg2']
    puts '@var is modified!!!'
    @var = 'var3'
    puts @var
  end

  def step2
    puts "Instance variables is initialized by each tasks."
    puts @var
  end
end
