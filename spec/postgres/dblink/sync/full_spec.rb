require 'spec_helper'

RSpec.describe Postgres::Dblink::Sync::Full do

  describe "#exec_sync" do
    let(:pg_result) { double("PG::Result", cmd_tuples: -3) }

    before do
      allow(subject).to receive(:execute_remote).and_return(pg_result)
      allow(subject).to receive(:query_full_select_into_table).and_return("full query select into table")
    end

    it "executes remote query" do
      expect(subject).to receive(:execute_remote).with("full query select into table")
      subject.exec_sync
    end
    it "sets row_count" do
      subject.exec_sync
      expect(subject.row_count).to eq(-3)
    end
  end #end "#exec_sync"

  describe "#query_full_select_into_table" do
    before do
      allow(subject).to receive(:table_name).and_return("the_table")
      allow(subject).to receive(:connection_name).and_return("the_conn_name")
      allow(subject).to receive(:remote_query).and_return("an escaped string with 'quoted' quotes")
      allow(subject).to receive(:query_part_table_definition).and_return("the, table, def")
      allow(subject).to receive(:query_part_column_names).and_return("the, column, names")
    end
    it "returns correct sql" do
      expected_sql = <<-SQL.strip
              INSERT INTO the_table
                (
                  the, column, names
                )
                SELECT
                  the, column, names
                FROM dblink(
                  'the_conn_name',
                  'an escaped string with ''quoted'' quotes'
                )
                AS t (the, table, def);
      SQL
      expect(subject.query_full_select_into_table).to eq(expected_sql)
    end
  end #end "#query_full_select_into_table"

end