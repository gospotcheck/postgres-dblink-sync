require 'spec_helper'

RSpec.describe Postgres::Dblink::Sync::Batch do

  describe "constants" do
    describe "DEFAULT_BATCH_SIZE" do
      it "returns size" do
        expect(described_class::DEFAULT_BATCH_SIZE).to eq(10000)
      end
    end
  end #end "constants"

  describe "#batch_size" do
    before do
      stub_const('Postgres::Dblink::Sync::Batch::DEFAULT_BATCH_SIZE', 'Groovy')
    end
    it "returns default batch size" do
      expect(subject.batch_size).to eq('Groovy')
    end
  end #end "#batch_size"

  describe "#exec_sync" do
    it "executes queries in correct order" do
      expect(subject).to receive(:exec_query_batch_open_cursor).ordered
      expect(subject).to receive(:exec_sync_batches).ordered
      expect(subject).to receive(:exec_query_batch_close_cursor).ordered
      subject.exec_sync
    end
  end #end "#exec_sync"

  describe "#exec_sync_batches" do
    let(:batch1) { double("Batch 1", cmd_tuples: 12) }
    let(:batch2) { double("Batch 2", cmd_tuples: 0) }

    before do
      allow(subject).to receive(:execute_remote).and_return(batch1, batch2)
      allow(subject).to receive(:query_batch_select_into_table).and_return("batch query select into table")
    end

    it "executes remote query until no more results are returned" do
      expect(subject).to receive(:execute_remote).with("batch query select into table").twice
      subject.exec_sync_batches
    end
    it "sets row_count" do
      subject.exec_sync_batches
      expect(subject.row_count).to eq(12)
    end
  end #end "#exec_sync_batches"

  describe "#exec_query_batch_open_cursor" do
    before do
      allow(subject).to receive(:query_batch_open_cursor).and_return('OPEN CURSOR QUERY;')
    end

    it "executes query_dblink_connect" do
      expect(subject).to receive(:execute_remote).with('OPEN CURSOR QUERY;')
      subject.exec_query_batch_open_cursor
    end
  end #end "#exec_query_batch_open_cursor"

  describe "#query_batch_open_cursor" do
    before do
      allow(subject).to receive(:connection_name).and_return('the_conn')
      allow(subject).to receive(:table_name).and_return('the_table')
      allow(subject).to receive(:remote_query).and_return("The remote 'query';")
    end

    it "returns correct sql" do
      expected_sql = <<-SQL.strip
            SELECT dblink_open(
              'the_conn',
              'the_table',
              'The remote ''query'';'
            );
      SQL
      expect(subject.query_batch_open_cursor).to eq(expected_sql)
    end
  end #end "#query_batch_open_cursor"

  describe "#query_batch_select_into_table" do
    before do
      allow(subject).to receive(:table_name).and_return('the_table')
      allow(subject).to receive(:query_batch_fetch).and_return('the_fetch_query')
      allow(subject).to receive(:query_part_column_names).and_return('the, column, names')
    end

    it "returns correct sql" do
      expected_sql = <<-SQL.strip
              -- Select into the_table table with remote data
              INSERT INTO the_table
                (
                  the, column, names
                )
                the_fetch_query;
      SQL
      expect(subject.query_batch_select_into_table).to eq(expected_sql)
    end
  end #end "#query_batch_select_into_table"

  describe "#query_batch_close_cursor" do
    before do
      allow(subject).to receive(:connection_name).and_return('the_conn')
      allow(subject).to receive(:table_name).and_return('the_table')
    end

    it "returns correct sql" do
      expected_sql = <<-SQL.strip
            SELECT dblink_close('the_conn', 'the_table')
      SQL
      expect(subject.query_batch_close_cursor).to eq(expected_sql)
    end
  end #end "#query_batch_close_cursor"

  describe "#query_batch_fetch" do
    before do
      stub_const('Postgres::Dblink::Sync::Batch::DEFAULT_BATCH_SIZE', '-5')
      allow(subject).to receive(:connection_name).and_return('the_conn')
      allow(subject).to receive(:table_name).and_return('the_table')
      allow(subject).to receive(:query_part_table_definition).and_return('the_table_def')
      allow(subject).to receive(:query_part_column_names).and_return('the, column, names')
    end

    it "returns correct sql" do
      expected_sql = <<-SQL.strip
              SELECT
                the, column, names
              FROM dblink_fetch(
                'the_conn',
                'the_table',
                -5
              )
              AS (the_table_def)
      SQL
      expect(subject.query_batch_fetch).to eq(expected_sql)
    end
  end #end "#query_batch_fetch"

  describe "#exec_query_batch_close_cursor" do
    before do
      allow(subject).to receive(:query_batch_close_cursor).and_return('CLOSE CURSOR QUERY;')
    end

    it "executes query_dblink_connect" do
      expect(subject).to receive(:execute_remote).with('CLOSE CURSOR QUERY;')
      subject.exec_query_batch_close_cursor
    end
  end #end "#exec_query_batch_close_cursor"

end