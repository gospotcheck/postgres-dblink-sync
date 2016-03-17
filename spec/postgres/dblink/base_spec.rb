require 'spec_helper'

RSpec.describe Postgres::Dblink::Sync::Base do

  describe "attr_accessor" do
    describe ":disabled_reason" do
      it "should be set" do
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
      allow(subject).to receive(:execute_remote)
    end

    describe "when synchronizer is valid" do
      before do
        allow(subject).to receive(:valid?).and_return(true)
        allow(subject).to receive(:query_full).and_return("THE_QUERY;")
      end

      it "resets :disabled_reason" do
        subject.disabled_reason = 'blah'
        expect(subject.disabled_reason).to eq('blah')
        subject.sync
        expect(subject.disabled_reason).to eq(nil)
      end
      it "executes correct query" do
        expect(subject).to receive(:execute_remote).with('THE_QUERY;')
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
        expect(subject).not_to receive(:execute_remote)
        subject.sync
      end
      it "returns false" do
        expect(subject.sync).to eq(false)
      end
    end
  end #end "#sync"

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

  describe "#query_enable_dblink" do
    it "should create dblink extension" do
      expected_sql = <<-SQL.strip
              -- Load extension
              CREATE EXTENSION IF NOT EXISTS dblink;
      SQL
      expect(subject.query_enable_dblink).to eq(expected_sql)
    end
  end #end "#query_enable_dblink"

  describe "#query_dblink_connection" do
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
      expect(subject.query_dblink_connection).to eq(expected_sql)
    end
  end #end "#query_dblink_connection"

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

  describe "#query_sync" do
    describe "when sync_type is :truncate" do
      before do
        allow(subject).to receive(:sync_type).and_return(:truncate)
        allow(subject).to receive(:query_sync_truncate).and_return("query_sync_truncate")
      end
      it "returns query_sync_truncate" do
        expect(subject.query_sync).to eq('query_sync_truncate')
      end
    end
    describe "when sync_type is :temp_table" do
      before do
        allow(subject).to receive(:sync_type).and_return(:temp_table)
        allow(subject).to receive(:query_sync_temp_table).and_return("query_sync_temp_table")
      end
      it "returns query_sync_temp_table" do
        expect(subject.query_sync).to eq('query_sync_temp_table')
      end
    end
    describe "when sync_type is invalid" do
      before do
        allow(subject).to receive(:sync_type).and_return(:your_mom)
      end
      it "raises ArgumentError" do
        expect { subject.query_sync }.to raise_error(ArgumentError, "You must override `sync_type' in your class as one of [:truncate, :temp_table]")
      end
    end
  end #end "#query_sync"

  describe "#query_sync_truncate" do
    before do
      allow(subject).to receive(:query_truncate_table).and_return("query_truncate_table")
      allow(subject).to receive(:query_select_into_table).and_return("query_select_into_table")
    end

    it "includes sub-queries in correct order" do
      expected_result = <<-STRING.strip
            query_truncate_table
            query_select_into_table
      STRING
      expect(subject.query_sync_truncate).to eq(expected_result)
    end
  end #end "#query_sync_truncate"

  describe "#query_sync_temp_table" do
    before do
      allow(subject).to receive(:query_drop_temp_table).and_return("query_drop_temp_table")
      allow(subject).to receive(:query_create_temp_table).and_return("query_create_temp_table")
      allow(subject).to receive(:query_create_primary_key_sequence).and_return("query_create_primary_key_sequence")
      allow(subject).to receive(:query_create_indexes).and_return("query_create_indexes")
      allow(subject).to receive(:query_move_temp_table_into_place).and_return("query_move_temp_table_into_place")
    end

    it "includes sub-queries in correct order" do
      expected_result = <<-STRING.strip
            query_drop_temp_table
            query_create_temp_table
            query_create_primary_key_sequence
            query_create_indexes
            query_move_temp_table_into_place
      STRING
      expect(subject.query_sync_temp_table).to eq(expected_result)
    end
  end #end "#query_sync_temp_table"

  describe "#sync_type" do
    it "raises error" do
      expect{subject.sync_type}.to raise_error("You must override `sync_type' in your class")
    end
  end #end "#sync_type"

  describe "#table_name" do
    it "raises error" do
      expect{subject.table_name}.to raise_error("You must override `table_name' in your class")
    end
  end #end "#table_name"

  describe "#temp_table_name" do
    before do
      allow(subject).to receive(:table_name).and_return("molecules")
    end
    it "appends _temp to table_name" do
      expect(subject.temp_table_name).to eq("molecules_temp")
    end
  end #end "#temp_table_name"

  describe "#sequence_name" do
    it "raises error" do
      expect{subject.sequence_name}.to raise_error("You must override `sequence_name' in your class")
    end
  end #end "#sequence_name"

  describe "#primary_key" do
    it "raises error" do
      expect{subject.primary_key}.to raise_error("You must override `primary_key' in your class")
    end
  end #end "#primary_key"

  describe "#query_remote" do
    it "raises error" do
      expect{subject.query_remote}.to raise_error("You must override `query_remote' in your class")
    end
  end #end "#query_remote"

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

  describe "#query_select_into_table" do
    before do
      allow(subject).to receive(:table_name).and_return('the_existing_table')
      allow(subject).to receive(:query_remote).and_return('the_query_remote')
    end
    it "returns correct sql" do
      expected_sql = <<-SQL.strip
            -- Select into the_existing_table table with remote data
            INSERT INTO the_existing_table
              the_query_remote;
      SQL
      expect(subject.query_select_into_table).to eq(expected_sql)
    end
  end #end "#query_select_into_table"

  describe "#query_create_temp_table" do
    before do
      allow(subject).to receive(:temp_table_name).and_return('a_temp_table')
      allow(subject).to receive(:query_remote).and_return("the_remote_query")
      allow(subject).to receive(:primary_key).and_return('the_primary_key')
    end
    it "should return correct sql" do
      expected_sql = <<-SQL.strip
            -- Create a_temp_table table with remote data
            CREATE TABLE a_temp_table
              AS
                the_remote_query
            ALTER TABLE a_temp_table ADD PRIMARY KEY (the_primary_key);
      SQL
      expect(subject.query_create_temp_table).to eq(expected_sql)
    end
  end #end "#query_create_temp_table"

  describe "#query_drop_temp_table" do
    before do
      allow(subject).to receive(:temp_table_name).and_return('the_temp_table')
    end
    it "should return correct sql" do
      expected_sql = <<-SQL.strip
            -- Drop the_temp_table if exists
            DROP TABLE IF EXISTS the_temp_table CASCADE;
      SQL
      expect(subject.query_drop_temp_table).to eq(expected_sql)
    end
  end #end "#query_drop_temp_table"

  describe "#query_create_primary_key_sequence" do
    before do
      allow(subject).to receive(:sequence_name).and_return("genome")
      allow(subject).to receive(:temp_table_name).and_return('ima_table')
      allow(subject).to receive(:primary_key).and_return('motley_id')
    end

    it "should return correct sql" do
      expected_sql = <<-SQL.strip
            -- Create sequence if it does not already exist
            DO $$
            BEGIN
              -- Check if sequence already exists
              IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'genome') THEN
                raise notice 'Using existing sequence: genome';
              ELSE
                -- Create sequence
                raise notice 'Creating sequence: genome';
                CREATE SEQUENCE genome
                  START WITH 1
                  INCREMENT BY 1
                  NO MINVALUE
                  NO MAXVALUE
                  CACHE 1;
              END IF;
            END$$;
            -- Add sequence to table and add constraints
            ALTER SEQUENCE genome OWNED BY ima_table.motley_id;
            ALTER TABLE ONLY ima_table ALTER COLUMN motley_id SET DEFAULT nextval('genome'::regclass);
      SQL
      expect(subject.query_create_primary_key_sequence).to eq(expected_sql)
    end
  end #end "#query_create_primary_key_sequence"

  describe "#query_create_indexes" do
    it "should do nothing" do
      expect(subject.query_create_indexes).to eq(nil)
    end
  end #end "#query_create_indexes"

  describe "#query_move_temp_table_into_place" do
    before do
      allow(subject).to receive(:table_name).and_return('eek')
      allow(subject).to receive(:temp_table_name).and_return('less')
    end
    it "should return correct sql" do
      expected_sql = <<-SQL.strip
            -- Move eek into place
            ALTER TABLE eek RENAME TO eek_old;
            ALTER TABLE less RENAME TO eek;
            DROP TABLE eek_old CASCADE;
      SQL
      expect(subject.query_move_temp_table_into_place).to eq(expected_sql)
    end
  end #end "#query_move_temp_table_into_place"

  describe "#valid?" do
    it "raises error" do
      expect{subject.valid?}.to raise_error("You must override `valid?' in your class")
    end
  end #end "#valid?"

end