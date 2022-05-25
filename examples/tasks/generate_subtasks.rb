# add_subtask(params)
module JustParams
  class ParallelProcess
    def split
      Digdag.env.store(task_count: 3)
    end

    def run(task_count)
      tasks = task_count.times.each_with_object({}) do |i, memo|
        memo["+task#{i}"] = {
          'rb>': 'JustParams::ParallelProcess.subtask',
          index: i
        }
      end
      tasks['_parallel'] = true
      Digdag.env.add_subtask(tasks)
    end
  end

  def subtask(index)
    puts("Processing" + index.to_s)
  end
end

# add_subtask(singleton_method_name, params={})
module WithSingletonMethod
  class ParallelProcess
    def split
      Digdag.env.store(task_count: 3)
    end

    def run(task_count)
      task_count.times do |i|
        Digdag.env.add_subtask(:subtask, index: i)
      end
      Digdag.env.subtask_config['_parallel'] = true
    end
  end
end

def subtask(index)
  puts("Processing" + index.to_s)
end

# add_subtask(klass, instance_method_name, params={})
module WithInstanceMethod
  class ParallelProcess
    def split
      Digdag.env.store(task_count: 3)
    end

    def run(task_count)
      task_count.times do |i|
        Digdag.env.add_subtask(ParallelProcess, :subtask, index: i)
      end
      Digdag.env.subtask_config['_parallel'] = true
    end

    def subtask(index)
      puts("Processing" + index.to_s)
    end
  end
end
