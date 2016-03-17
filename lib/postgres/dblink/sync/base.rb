require 'postgres/dblink/sync/utils'

module Postgres
  module Dblink
    module Sync
      class Base

        #In case we can't synchronize, we can find out why
        attr_accessor :disabled_reason

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
            execute_remote(query_full)
            true
          else
            false
          end
        end

        #Includes dblink setup, connection and query
        def query_full
          query_enable_dblink + query_dblink_connection + query_sync
        end

        #Enables the dblink extension
        def query_enable_dblink
          <<-SQL.strip
              -- Load extension
              CREATE EXTENSION IF NOT EXISTS dblink;
          SQL
        end

        #Returns the dblink connection query based on the follower database url
        def query_dblink_connection
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

        #Used in the dblink connections
        def connection_name
          raise "You must override `connection_name' in your class"
        end

        #Loads the actual URL from the given database variable
        def remote_database_url
          raise "You must override `remote_database_url' in your class"
        end

        #Returns sync query based on sync_type
        def query_sync
          case sync_type
            when :truncate
              query_sync_truncate
            when :temp_table
              query_sync_temp_table
            else
              raise ArgumentError, "You must override `sync_type' in your class as one of [:truncate, :temp_table]"
          end
        end

        #Generates SQL based on table truncation
        def query_sync_truncate
          <<-SQL.strip
            #{query_truncate_table}
            #{query_select_into_table}
          SQL
        end

        #Generates the sql required to synchronize this table, assuming the dblink extension and connection already exist
        def query_sync_temp_table
          <<-SQL.strip
            #{query_drop_temp_table}
            #{query_create_temp_table}
            #{query_create_primary_key_sequence}
            #{query_create_indexes}
            #{query_move_temp_table_into_place}
          SQL
        end

        #Either :truncation or :temp_table
        def sync_type
          raise "You must override `sync_type' in your class"
        end

        #The name of the table
        def table_name
          raise "You must override `table_name' in your class"
        end

        #Automatic temp table name
        def temp_table_name
          "#{table_name}_temp"
        end

        #The name of the sequence to be used for the table, i.e. 'mission_responses_id_seq'
        def sequence_name
          raise "You must override `sequence_name' in your class"
        end

        #The name of the primary key for the table, typically 'id'
        def primary_key
          raise "You must override `primary_key' in your class"
        end

        #The remote query to pull data into this table
        def query_remote
          raise "You must override `query_remote' in your class"
        end

        def query_truncate_table
          <<-SQL.strip
            -- Truncate table #{table_name}
            TRUNCATE TABLE #{table_name};
          SQL
        end

        #Selects into an existing table
        def query_select_into_table
          <<-SQL.strip
            -- Select into #{table_name} table with remote data
            INSERT INTO #{table_name}
              #{query_remote};
          SQL
        end

        #Drops the temp table for the overridden class
        def query_drop_temp_table
          <<-SQL.strip
            -- Drop #{temp_table_name} if exists
            DROP TABLE IF EXISTS #{temp_table_name} CASCADE;
          SQL
        end

        #Creates the temporary table for the overridden class
        def query_create_temp_table
          <<-SQL.strip
            -- Create #{temp_table_name} table with remote data
            CREATE TABLE #{temp_table_name}
              AS
                #{query_remote}
            ALTER TABLE #{temp_table_name} ADD PRIMARY KEY (#{primary_key});
          SQL
        end

        #Creates the sequence and adds it to the primary key of the table
        def query_create_primary_key_sequence
          <<-SQL.strip
            -- Create sequence if it does not already exist
            DO $$
            BEGIN
              -- Check if sequence already exists
              IF EXISTS (SELECT 1 FROM pg_class WHERE relname = '#{sequence_name}') THEN
                raise notice 'Using existing sequence: #{sequence_name}';
              ELSE
                -- Create sequence
                raise notice 'Creating sequence: #{sequence_name}';
                CREATE SEQUENCE #{sequence_name}
                  START WITH 1
                  INCREMENT BY 1
                  NO MINVALUE
                  NO MAXVALUE
                  CACHE 1;
              END IF;
            END$$;
            -- Add sequence to table and add constraints
            ALTER SEQUENCE #{sequence_name} OWNED BY #{temp_table_name}.#{primary_key};
            ALTER TABLE ONLY #{temp_table_name} ALTER COLUMN #{primary_key} SET DEFAULT nextval('#{sequence_name}'::regclass);
          SQL
        end

        #Creates the indexes on the temporary table
        def query_create_indexes
        end

        #Renames old table, moves temp table into place, deletes old table
        def query_move_temp_table_into_place
          <<-SQL.strip
            -- Move #{table_name} into place
            ALTER TABLE #{table_name} RENAME TO #{table_name}_old;
            ALTER TABLE #{temp_table_name} RENAME TO #{table_name};
            DROP TABLE #{table_name}_old CASCADE;
          SQL
        end

        #Called to ensure query has everything it needs to run
        def valid?
          raise "You must override `valid?' in your class"
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

        #Executes the given query string in the remote dblink database by whatever means
        def execute_remote(query)
          raise "You must override `execute_remote' in your class"
        end
      end
    end
  end
end