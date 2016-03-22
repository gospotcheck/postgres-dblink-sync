require 'spec_helper'

RSpec.describe Postgres::Dblink::Sync::Base do

  describe "constants" do
    describe "DEFAULT_BATCH_SIZE" do
      it "returns size" do
        expect(described_class::DEFAULT_BATCH_SIZE).to eq(10000)
      end
    end
  end #end "constants"

  describe "attr_accessor" do
    describe ":disabled_reason" do
      it "exists" do
        expect(subject).to respond_to(:disabled_reason)
        expect(subject).to respond_to(:disabled_reason=)
      end
    end
  end #end "attr_accessor"

  describe ".sync" do
    let(:instance) { subject }
    it "executes sync on instance and returns result" do
      expect(described_class).to receive(:new).and_return(instance)
      expect(instance).to receive(:sync).and_return('the_result')
      expect(described_class.sync).to eq('the_result')
    end
  end #end ".sync"

  describe "#sync" do
    before do
      allow(subject).to receive(:execute_sync)
    end

    describe "when synchronizer is valid" do
      before do
        allow(subject).to receive(:valid?).and_return(true)
      end

      it "resets :disabled_reason" do
        subject.disabled_reason = 'blah'
        expect(subject.disabled_reason).to eq('blah')
        subject.sync
        expect(subject.disabled_reason).to eq(nil)
      end
      it "executes sync" do
        expect(subject).to receive(:execute_sync)
        subject.sync
      end
      it "returns true" do
        expect(subject.sync).to eq(true)
      end
    end
    describe "when synchronizer is invalid" do
      before do
        allow(subject).to receive(:valid?).and_return(false)
      end

      it "does not attempt to sync" do
        expect(subject).not_to receive(:execute_sync)
        subject.sync
      end
      it "returns false" do
        expect(subject.sync).to eq(false)
      end
    end
  end #end "#sync"

  describe "#execute_sync" do
    it "performs sync actions in order" do
      expect(subject).to receive(:exec_query_dblink_connect).ordered
      expect(subject).to receive(:exec_query_open_cursor).ordered
      expect(subject).to receive(:exec_query_truncate_table).ordered
      expect(subject).to receive(:exec_remote_query_in_batches).ordered
      expect(subject).to receive(:exec_query_close_cursor).ordered
      subject.execute_sync
    end
  end #end "#execute_sync"

  describe "#batch_size" do
    before do
      stub_const('Postgres::Dblink::Sync::Base::DEFAULT_BATCH_SIZE', 'Groovy')
    end
    it "returns default batch size" do
      expect(subject.batch_size).to eq('Groovy')
    end
  end #end "#batch_size"

  describe "#connection_name" do
    it "raises error" do
      expect{subject.connection_name}.to raise_error("You must override `connection_name' in your class")
    end
  end #end "#connection_name"

  describe "#remote_database_url" do
    it "raises error" do
      expect{subject.remote_database_url}.to raise_error("You must override `remote_database_url' in your class")
    end
  end #end "#remote_database_url"

  describe "#table_name" do
    it "raises error" do
      expect{subject.table_name}.to raise_error("You must override `table_name' in your class")
    end
  end #end "#table_name"

  describe "#remote_query" do
    it "raises error" do
      expect{subject.remote_query}.to raise_error("You must override `remote_query' in your class")
    end
  end #end "#remote_query"

  describe "#remote_query_column_types" do
    it "raises error" do
      expect{subject.remote_query_column_types}.to raise_error("You must override `remote_query_column_types' in your class")
    end
  end #end "#remote_query_column_types"

  describe "#execute_remote" do
    it "raises error" do
      expect{subject.execute_remote('the query')}.to raise_error("You must override `execute_remote' in your class")
    end
  end #end "#execute_remote"

  describe "#valid?" do
    it "raises error" do
      expect{subject.valid?}.to raise_error("You must override `valid?' in your class")
    end
  end #end "#valid?"

  describe "#exec_query_dblink_connect" do
    before do
      allow(subject).to receive(:query_enable_dblink).and_return('ENABLE QUERY;')
      allow(subject).to receive(:query_dblink_connect).and_return('CONNECT QUERY;')
    end

    it "executes query_dblink_connect" do
      expect(subject).to receive(:execute_remote).with('ENABLE QUERY;CONNECT QUERY;')
      subject.exec_query_dblink_connect
    end
  end #end "#exec_query_dblink_connect"

  describe "#exec_query_open_cursor" do
    before do
      allow(subject).to receive(:query_open_cursor).and_return('OPEN CURSOR QUERY;')
    end

    it "executes query_dblink_connect" do
      expect(subject).to receive(:execute_remote).with('OPEN CURSOR QUERY;')
      subject.exec_query_open_cursor
    end
  end #end "#exec_query_open_cursor"

  describe "#exec_query_truncate_table" do
    before do
      allow(subject).to receive(:query_truncate_table).and_return('TRUNCATE QUERY;')
    end

    it "executes query_dblink_connect" do
      expect(subject).to receive(:execute_remote).with('TRUNCATE QUERY;')
      subject.exec_query_truncate_table
    end
  end #end "#exec_query_open_cursor"

  describe "#exec_remote_query_in_batches" do
    let(:batch1) { double("Batch 1", count: 12) }
    let(:batch2) { double("Batch 2", count: 0) }

    before do
      allow(subject).to receive(:execute_remote).and_return(batch1, batch2)
      allow(subject).to receive(:query_select_batch_into_table).and_return("batch query select into table")
    end

    it "executes remote query until no more results are returned" do
      expect(subject).to receive(:execute_remote).with("batch query select into table").twice
      subject.exec_remote_query_in_batches
    end

  end #end "#exec_remote_query_in_batches"

  describe "#exec_query_close_cursor" do
    before do
      allow(subject).to receive(:query_close_cursor).and_return('CLOSE CURSOR QUERY;')
    end

    it "executes query_dblink_connect" do
      expect(subject).to receive(:execute_remote).with('CLOSE CURSOR QUERY;')
      subject.exec_query_close_cursor
    end
  end #end "#exec_query_close_cursor"

  describe "#query_enable_dblink" do
    it "should create dblink extension" do
      expected_sql = <<-SQL.strip
              -- Load extension
              CREATE EXTENSION IF NOT EXISTS dblink;
      SQL
      expect(subject.query_enable_dblink).to eq(expected_sql)
    end
  end #end "#query_enable_dblink"

  describe "#query_dblink_connect" do
    before do
      allow(subject).to receive(:connection_name).and_return('THE_VARIABLE_NAME')
      allow(subject).to receive(:remote_database_url).and_return('the_follower_url')
    end

    it "should return correct sql" do
      expect(subject).to receive(:get_connection_string).with('the_follower_url').and_return('the_connection_string')
      expected_sql = <<-SQL.strip
              -- Connect to remote db
              DO $$
              DECLARE
                -- Get existing connections
                conns text[] := dblink_get_connections();
              BEGIN
                -- Check if connection already exists
                IF conns @> ARRAY['THE_VARIABLE_NAME'::text] THEN
                  raise notice 'Using existing connection: %', conns;
                ELSE
                  -- Make connection
                  PERFORM dblink_connect('THE_VARIABLE_NAME', 'the_connection_string');
                END IF;
              END$$;
      SQL
      expect(subject.query_dblink_connect).to eq(expected_sql)
    end
  end #end "#query_dblink_connect"

  describe "#query_open_cursor" do
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
      expect(subject.query_open_cursor).to eq(expected_sql)
    end
  end #end "#query_open_cursor"

  describe "#query_close_cursor" do
    before do
      allow(subject).to receive(:connection_name).and_return('the_conn')
      allow(subject).to receive(:table_name).and_return('the_table')
    end

    it "returns correct sql" do
      expected_sql = <<-SQL.strip
            SELECT dblink_close('the_conn', 'the_table')
      SQL
      expect(subject.query_close_cursor).to eq(expected_sql)
    end
  end #end "#query_close_cursor"

  describe "#query_fetch_batch" do
    before do
      stub_const('Postgres::Dblink::Sync::Base::DEFAULT_BATCH_SIZE', '-5')
      allow(subject).to receive(:connection_name).and_return('the_conn')
      allow(subject).to receive(:table_name).and_return('the_table')
      allow(subject).to receive(:query_part_table_definition).and_return('the_table_def')
    end

    it "returns correct sql" do
      expected_sql = <<-SQL.strip
            SELECT
              *
            FROM dblink_fetch(
              'the_conn',
              'the_table',
              -5
            )
            AS (the_table_def)
      SQL
      expect(subject.query_fetch_batch).to eq(expected_sql)
    end
  end #end "#query_fetch_batch"

  describe "#query_part_table_definition" do
    let(:remote_query_column_types) do
      [
        ['column', 'type'],
        ['id', 'word']
      ]
    end
    before do
      allow(subject).to receive(:remote_query_column_types).and_return(remote_query_column_types)
    end

    it "returns correct string" do
      expect(subject.query_part_table_definition).to eq('column type, id word')
    end
  end #end "#query_part_table_definition"

  describe "#query_select_batch_into_table" do
    before do
      allow(subject).to receive(:table_name).and_return('the_table')
      allow(subject).to receive(:query_fetch_batch).and_return('the_fetch_query')
    end

    it "returns correct sql" do
      expected_sql = <<-SQL.strip
            -- Select into the_table table with remote data
            INSERT INTO the_table
              the_fetch_query
            RETURNING 1;
      SQL
      expect(subject.query_select_batch_into_table).to eq(expected_sql)
    end
  end #end "#query_select_batch_into_table"

  describe "#query_truncate_table" do
    before do
      allow(subject).to receive(:table_name).and_return('the_existing_table')
    end
    it "returns correct sql" do
      expected_sql = <<-SQL.strip
            -- Truncate table the_existing_table
            TRUNCATE TABLE the_existing_table;
      SQL
      expect(subject.query_truncate_table).to eq(expected_sql)
    end
  end #end "#query_truncate_table"

  describe "#get_connection_string" do

    subject { described_class.new.get_connection_string(url) }

    describe "when host and dbname present" do
      let(:url) { "postgres://localhost/the_database"}
      it "should return correct string" do
        expect(subject).to eq("host=localhost dbname=the_database")
      end

      describe "when port present" do
        let(:url) { "postgres://hostname:452/yar" }
        it "should return correct string" do
          expect(subject).to eq("host=hostname dbname=yar port=452")
        end
      end
      describe "when user present" do
        let(:url) { "postgres://adam@meek/ly" }
        it "should return correct string" do
          expect(subject).to eq("host=meek dbname=ly user=adam")
        end
      end
      describe "when password present" do
        let(:url) { "postgres://jane:woot@word/march" }
        it "should return correct string" do
          expect(subject).to eq("host=word dbname=march user=jane password=woot")
        end
      end
      describe "when all present" do
        let(:url) { "postgres://i:do@not:1337/care"}
        it "should return correct string" do
          expect(subject).to eq("host=not dbname=care port=1337 user=i password=do")
        end
      end
    end
  end #end "#get_connection_string"

end