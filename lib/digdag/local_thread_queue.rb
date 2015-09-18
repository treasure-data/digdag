
module Digdag
  require 'concurrent'

  class LocalThreadQueue
    def initialize(action_runner)
      @pool = Concurrent::ThreadPoolExecutor.new
      @action_runner = action_runner
    end

    def submit(action)
      @pool.post do
        begin
          @action_runner.run(action)
        rescue => e
          STDERR.puts "Unexpected error: #{e}"
          e.backtrace.each do |bt|
            STDERR.puts "  #{bt}"
          end
        end
      end
    end
  end

end
