
module Digdag
  module Tasks

    class TdBaseTask < BaseTask
      def init
        require 'td-client'
        @apikey = config.param(:apikey, :string)
        @endpoint = config.param(:endpoint, :string, default: "api.treasuredata.com")
        @database = config.param(:database, :string)
        @client = TreasureData::Client.new(@apikey, endpoint: @endpoint)
      end

      def ensure_table_deleted(table)
        # TODO retry
        begin
          @client.delete_table(@database, table)
        rescue TreasureData::NotFoundError
          # ignore
        end
      end

      def ensure_table_exists(table)
        # TODO retry
        begin
          @client.create_log_table(@database, table)
        rescue TreasureData::AlreadyExistsError
          # ignore
        end
      end
    end

  end
end
