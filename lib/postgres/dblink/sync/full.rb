module Postgres::Dblink::Sync
  class Full < Base

    #Execute sync in one query
    def exec_sync
      res = execute_remote(query_full_select_into_table)
      self.row_count = res.cmd_tuples
      res
    end

    def query_full_select_into_table
      <<-SQL.strip
              INSERT INTO #{table_name}
                (
                  #{query_part_column_names}
                )
                SELECT
                  #{query_part_column_names}
                FROM dblink(
                  '#{connection_name}',
                  '#{PG::Connection.escape_string(remote_query)}'
                )
                AS t (#{query_part_table_definition});
      SQL
    end
  end
end
