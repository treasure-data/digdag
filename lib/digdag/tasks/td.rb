
module Digdag
  module Tasks
    require 'delegate'

    class TdTask < TdBaseTask
      Plugin.register_task(:td, self)

      def run
        insert_into = config.param(:insert_into, :string, default: nil)
        create_table = config.param(:create_table, :string, default: nil)

        query = config.param(:query, :string, alias: :command)
        query_type = config.param(:query_type, :string, default: "presto")

        options = {
          priority: config.param(:priority, :integer, default: 0),
          result: config.param(:result, :string, default: nil),
        }

        case query_type
        when "hive"
          if insert_into
            ensure_table_exists(insert_into)
            q = "INSERT INTO TABLE #{insert_into}\n#{query}"
          elsif create_table
            ensure_table_deleted(create_table)
            ensure_table_exists(create_table)
            q = "INSERT OVERWRITE INTO TABLE #{create_table}\n#{query}"
          else
            q = query
          end
          options[:type] = :hive

        when "presto"
          if insert_into
            ensure_table_exists(insert_into)
            q = "INSERT INTO #{insert_into}\n#{query}"
          elsif create_table
            ensure_table_deleted(create_table)
            q = "CREATE TABLE #{create_table} AS\n#{query}"
          else
            q = query
          end
          options[:type] = :presto

        else
          raise ConfigError, "Unknown query_type. Available types are presto and hive: #{query_type.to_s.dump}"
        end

        LOG.info "Running #{query_type} query: #{q}"
        query = Query.new @client.query(@database, q, options.delete(:result), options.delete(:priority), 0, options)

        query.get
      end

      class Query < SimpleDelegator
        def initialize(job, params={})
          super(job)
          @job = job
          @client = job.client
          @params = params
        end

        def kill
          @client.kill(@job.job_id)
          self
        end

        def join
          last_log = Time.now
          interval = 2
          until @job.finished?
            now = Time.now
            if last_log + 300 < now
              last_log = now
            end
            LOG.debug "sleep #{interval} seconds..."
            sleep interval
            interval = [interval * 2, 30].min
            @job.update_progress!
          end
          @job.update_status!
        end

        def get
          join
          unless @job.success?
            raise QueryFailedError.new(self, @job.status, @params)
          end
          return QueryResult.new(@job)
        end

        def cmdout
          (@job.debug || {})['cmdout'].to_s
        end

        def stderr
          (@job.debug || {})['stderr'].to_s
        end

        def message
          cmdout + stderr
        end
      end
    end

    class QueryFailedError < StandardError
      def initialize(query, status, params={})
        super("Test query #{query.job_id} failed with #{{status: status}.merge(params).map {|k,v| "#{k}=#{v}" }.join(' ')}:\n#{query.cmdout}\n#{query.stderr}")
        @query = query
      end

      attr_reader :query
    end

  end
end
