require 'postgres/dblink/sync/utils'
require 'pg/connection'

module Postgres
  module Dblink
    module Sync
      class Base

        #In case we can't synchronize, we can find out why
        attr_accessor :disabled_reason, :row_count

        class << self

          #Sync the table using an instance
          def sync
            self.new.sync
          end

        end

        #Fully synchronizes this class
        def sync
          self.disabled_reason = nil
          if valid?
            execute_sync
            true
          else
            false
          end
        end

        #Synchronizes data
        def execute_sync
          exec_query_dblink_connect
          exec_query_truncate_table if insert_type.to_sym == :truncate
          exec_sync
        end

        #Used in sub-classes to execute their query type
        def exec_sync
          raise "You must override `exec_sync' in your class"
        end

        #Used in the dblink connections
        def connection_name
          raise "You must override `connection_name' in your class"
        end

        #Loads the actual URL from the given database variable
        def remote_database_url
          raise "You must override `remote_database_url' in your class"
        end

        #The name of the table
        def table_name
          raise "You must override `table_name' in your class"
        end

        #The remote query to pull data into this table
        def remote_query
          raise "You must override `remote_query' in your class"
        end

        #Returns an array of arrays of [column_name, column_type]
        def remote_query_column_types
          raise "You must override `remote_query_column_types' in your class"
        end

        #Executes the given query string in the remote dblink database by whatever means
        def execute_remote(query)
          raise "You must override `execute_remote' in your class"
        end

        #Called to ensure query has everything it needs to run
        def valid?
          raise "You must override `valid?' in your class"
        end

        #Determines if we truncate the table before inserting
        def insert_type
          raise "You must override `insert_type' in your class to be one of [:truncate, :append]"
        end

        ############################################################
        ## Base query wrappers used by different mixins and types
        ############################################################

        #Executes the truncate query
        def exec_query_truncate_table
          execute_remote(query_truncate_table)
        end

        def query_truncate_table
          <<-SQL.strip
            -- Truncate table #{table_name}
            TRUNCATE TABLE #{table_name};
          SQL
        end

        #Executes the dblink enable and connect queries
        def exec_query_dblink_connect
          execute_remote(query_enable_dblink + query_dblink_connect)
        end

        #Enables the dblink extension
        def query_enable_dblink
          <<-SQL.strip
              -- Load extension
              CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;
          SQL
        end

        #Returns the dblink connection query based on the follower database url
        def query_dblink_connect
          #Get connection string from database url
          connection_string = get_connection_string(remote_database_url)
          #Ensure that we use an existing connection if we run within the same session
          <<-SQL.strip
              -- Connect to remote db
              DO $$
              DECLARE
                -- Get existing connections
                conns text[] := dblink_get_connections();
              BEGIN
                -- Check if connection already exists
                IF conns @> ARRAY['#{connection_name}'::text] THEN
                  raise notice 'Using existing connection: %', conns;
                ELSE
                  -- Make connection
                  PERFORM dblink_connect('#{connection_name}', '#{connection_string}');
                END IF;
              END$$;
          SQL
        end

        #Returns the table definition based on the remote_query_column_types
        def query_part_table_definition
          remote_query_column_types.map do |name, type|
            "#{name} #{type}"
          end.join("\n              , ")
        end

        #Returns the column names for the query for select order
        def query_part_column_names
          remote_query_column_types.map{|name, type| name}.join("\n                  , ")
        end

        #Get dblink connection string from database url
        def get_connection_string(url)
          conn = Utils.parse_connection_url(url)
          str = "host=#{conn[:host]} dbname=#{conn[:db]}"
          str << " port=#{conn[:port]}" if conn[:port].present?
          str << " user=#{conn[:user]}" if conn[:user].present?
          str << " password=#{conn[:pass]}" if conn[:pass].present?
          str
        end

      end
    end
  end
end