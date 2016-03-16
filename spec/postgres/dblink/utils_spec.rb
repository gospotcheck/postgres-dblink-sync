require 'spec_helper'

RSpec.describe Postgres::Dblink::Sync::Utils do

  describe ".parse_connection_url" do

    subject { described_class.parse_connection_url(url) }

    describe "with host and db" do
      let(:url) { "postgres://imahost/imadb" }

      it "should set :host" do
        expect(subject[:host]).to eq("imahost")
      end
      it "should set :db" do
        expect(subject[:db]).to eq("imadb")
      end
      it "should not set :port" do
        expect(subject[:port]).to eq(nil)
      end
      it "should not set :user" do
        expect(subject[:user]).to eq(nil)
      end
      it "should not set :pass" do
        expect(subject[:pass]).to eq(nil)
      end
    end

    describe "when host, port and db" do
      let(:url) { "postgres://hostme:8901/dbme" }

      it "should set :host" do
        expect(subject[:host]).to eq("hostme")
      end
      it "should set :db" do
        expect(subject[:db]).to eq("dbme")
      end
      it "should set :port" do
        expect(subject[:port]).to eq("8901")
      end
      it "should not set :user" do
        expect(subject[:user]).to eq(nil)
      end
      it "should not set :pass" do
        expect(subject[:pass]).to eq(nil)
      end
    end

    describe "when host, port, db, and user" do
      let(:url) { "postgres://shazam@whereami.com:4800/righthere" }

      it "should set :host" do
        expect(subject[:host]).to eq("whereami.com")
      end
      it "should set :db" do
        expect(subject[:db]).to eq("righthere")
      end
      it "should set :port" do
        expect(subject[:port]).to eq("4800")
      end
      it "should set :user" do
        expect(subject[:user]).to eq("shazam")
      end
      it "should not set :pass" do
        expect(subject[:pass]).to eq(nil)
      end
    end

    describe "when host, port, db, user, and pass" do
      let(:url) { "postgres://fookami:gewd@henna-hyphen-3.org:6700/anotherone" }

      it "should set :host" do
        expect(subject[:host]).to eq("henna-hyphen-3.org")
      end
      it "should set :db" do
        expect(subject[:db]).to eq("anotherone")
      end
      it "should set :port" do
        expect(subject[:port]).to eq("6700")
      end
      it "should set :user" do
        expect(subject[:user]).to eq("fookami")
      end
      it "should set :pass" do
        expect(subject[:pass]).to eq("gewd")
      end
    end

    describe "when host, db, user, and pass" do
      let(:url) { "postgres://lame:thing@bogus.com/goforit" }

      it "should set :host" do
        expect(subject[:host]).to eq("bogus.com")
      end
      it "should set :db" do
        expect(subject[:db]).to eq("goforit")
      end
      it "should not set :port" do
        expect(subject[:port]).to eq(nil)
      end
      it "should set :user" do
        expect(subject[:user]).to eq("lame")
      end
      it "should set :pass" do
        expect(subject[:pass]).to eq("thing")
      end
    end
  end #end ".parse_connection_url"

end