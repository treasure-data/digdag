class StacktraceRuby
  class MyErrorClass < StandardError; end

  def run
    private_run
  end

  private

  def private_run
    raise MyErrorClass.new('my error message')
  end
end
