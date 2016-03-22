require 'postgres/dblink/sync/utils'
require 'pg/connection'

module Postgres
  module Dblink
    module Sync
      class Base

        DEFAULT_BATCH_SIZE = 10000

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

        #Synchronizes data over in BATCH_SIZE batches
        def execute_sync
          exec_query_dblink_connect
          exec_query_open_cursor
          exec_query_truncate_table
          exec_remote_query_in_batches
          exec_query_close_cursor
        end

        def batch_size
          DEFAULT_BATCH_SIZE
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

        def exec_query_dblink_connect
          execute_remote(query_enable_dblink + query_dblink_connect)
        end

        def exec_query_open_cursor
          execute_remote(query_open_cursor)
        end

        def exec_query_truncate_table
          execute_remote(query_truncate_table)
        end

        def exec_remote_query_in_batches
          #Iterate until we have no more rows
          has_more_rows = true
          self.row_count = 0
          while has_more_rows
            res = execute_remote(query_select_batch_into_table)
            has_more_rows = res.count > 0
            self.row_count += res.count
          end
        end

        def exec_query_close_cursor
          execute_remote(query_close_cursor)
        end

        #Enables the dblink extension
        def query_enable_dblink
          <<-SQL.strip
              -- Load extension
              CREATE EXTENSION IF NOT EXISTS dblink;
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

        #Opens the remote cursor in the database
        def query_open_cursor
          <<-SQL.strip
            SELECT dblink_open(
              '#{connection_name}',
              '#{table_name}',
              '#{PG::Connection.escape_string(remote_query)}'
            );
          SQL
        end

        #Closes the remote cursor in the database
        def query_close_cursor
          <<-SQL.strip
            SELECT dblink_close('#{connection_name}', '#{table_name}')
          SQL
        end

        #Query to select a batch of size #batch_size
        def query_fetch_batch
          <<-SQL.strip
            SELECT
              *
            FROM dblink_fetch(
              '#{connection_name}',
              '#{table_name}',
              #{batch_size}
            )
            AS (#{query_part_table_definition})
          SQL
        end

        def query_part_table_definition
          remote_query_column_types.map do |name, type|
            "#{name} #{type}"
          end.join(", ")
        end

        #Selects into an existing table
        def query_select_batch_into_table
          <<-SQL.strip
            -- Select into #{table_name} table with remote data
            INSERT INTO #{table_name}
              #{query_fetch_batch}
            RETURNING 1;
          SQL
        end

        def query_truncate_table
          <<-SQL.strip
            -- Truncate table #{table_name}
            TRUNCATE TABLE #{table_name};
          SQL
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