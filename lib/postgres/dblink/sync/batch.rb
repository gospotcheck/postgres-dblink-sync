module Postgres::Dblink::Sync
  class Batch < Base

    DEFAULT_BATCH_SIZE = 10000

    def batch_size
      DEFAULT_BATCH_SIZE
    end

    #Run the sync in batches
    def exec_sync
      #Open cursor
      exec_query_batch_open_cursor
      #Insert in batches
      exec_sync_batches
      #Close cursor
      exec_query_batch_close_cursor
    end

    #Executes batches until done
    def exec_sync_batches
      #Iterate until we have no more rows
      has_more_rows = true
      self.row_count = 0
      while has_more_rows
        res = execute_remote(query_batch_select_into_table)
        has_more_rows = res.cmd_tuples > 0
        self.row_count += res.cmd_tuples
      end
    end

    #Opens cursor in remote db
    def exec_query_batch_open_cursor
      execute_remote(query_batch_open_cursor)
    end

    #Query to open the remote cursor in the database
    def query_batch_open_cursor
      <<-SQL.strip
            SELECT dblink_open(
              '#{connection_name}',
              '#{table_name}',
              '#{PG::Connection.escape_string(remote_query)}'
            );
      SQL
    end

    #Selects into an existing table
    def query_batch_select_into_table
      <<-SQL.strip
              -- Select into #{table_name} table with remote data
              INSERT INTO #{table_name}
                #{query_batch_fetch};
      SQL
    end

    #Query to select a batch of size #batch_size
    def query_batch_fetch
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

    #Closes cursor in remote db
    def exec_query_batch_close_cursor
      execute_remote(query_batch_close_cursor)
    end

    #Query to close the remote cursor in the database
    def query_batch_close_cursor
      <<-SQL.strip
              SELECT dblink_close('#{connection_name}', '#{table_name}')
      SQL
    end



  end
end
