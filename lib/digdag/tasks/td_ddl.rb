
module Digdag
  module Tasks

    class TdDdlTask < TdBaseTask
      Plugin.register_task(:td_ddl, self)

      def init
        require 'td-client'
        @apikey = config.param(:apikey, :string)
        @endpoint = config.param(:endpoint, :string, "api.treasuredata.com")
        @database = config.param(:database, :string)
        @client = TreasureData::Client.new(@apikey, endpoint: @endpoint)
      end

      def run
        # DDL
        drop_table = config.param(:drop_table, [:string], default: [])
        create_table = config.param(:create_table, [:string], default: [])
        empty_table = config.param(:empty_table, [:string], default: [])

        (drop_table + empty_table).each do |dt|
          ensure_table_deleted(dt)
        end

        # create_table
        (empty_table + create_table).each do |dt|
          ensure_table_exists(dt)
        end
      end
    end

  end
end
