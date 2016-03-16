module Postgres
  module Dblink
    module Sync
      class Utils

        class << self

          #Parses a postgres connection url into component parts
          def parse_connection_url(url)
            #Parses postgres://(user):(password)@(host):(port)/(dbname)
            match = url.match(/^postgres:\/\/(([^:]+?)?(:\S+)?@)?([^:]+)(:\S+)?\/(\S+)$/)
            {
              host: match[4],
              db: match[6],
              port: match[5].try(:gsub, /:/, ''),
              user: match[2],
              pass: match[3].try(:gsub, /:/, ''),
            }
          end

        end

      end
    end
  end
end

