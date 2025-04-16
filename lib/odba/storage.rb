#!/usr/bin/env ruby

# ODBA::Storage -- odba -- 08.12.2011 -- mhatakeyama@ywesee.com
# ODBA::Storage -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

require "singleton"
require "dbi"

module ODBA
  class Storage # :nodoc: all
    include Singleton
    attr_writer :dbi
    BULK_FETCH_STEP = 2500
    TABLES = [
      # in table 'object', the isolated dumps of all objects are stored
      ["object", <<~SQL],
        CREATE TABLE IF NOT EXISTS object (
          odba_id INTEGER NOT NULL, content TEXT,
          name TEXT, prefetchable BOOLEAN, extent TEXT,
          PRIMARY KEY(odba_id), UNIQUE(name)
        );
      SQL
      ["prefetchable_index", <<~SQL],
        CREATE INDEX IF NOT EXISTS prefetchable_index ON object(prefetchable);
      SQL
      ["extent_index", <<~SQL],
        CREATE INDEX IF NOT EXISTS extent_index ON object(extent);
      SQL
      # helper table 'object_connection'
      ["object_connection", <<~SQL],
        CREATE TABLE IF NOT EXISTS object_connection (
          origin_id integer, target_id integer,
          PRIMARY KEY(origin_id, target_id)
        );
      SQL
      ["target_id_index", <<~SQL],
        CREATE INDEX IF NOT EXISTS target_id_index ON object_connection(target_id);
      SQL
      # helper table 'collection'
      ["collection", <<~SQL]
        CREATE TABLE IF NOT EXISTS collection (
          odba_id integer NOT NULL, key text, value text,
          PRIMARY KEY(odba_id, key)
        );
      SQL
    ]
    def initialize
      @id_mutex = Mutex.new
    end

    def bulk_restore(bulk_fetch_ids)
      if bulk_fetch_ids.empty?
        []
      else
        bulk_fetch_ids = bulk_fetch_ids.uniq
        rows = []
        until (ids = bulk_fetch_ids.slice!(0, BULK_FETCH_STEP)).empty?
          sql = <<-SQL
						SELECT odba_id, content FROM object 
						WHERE odba_id IN (#{ids.join(",")})
          SQL
          rows.concat(dbi.select_all(sql))
        end
        rows
      end
    end

    def collection_fetch(odba_id, key_dump)
      sql = <<-SQL
        SELECT value FROM collection 
        WHERE odba_id = ? AND key = ?
      SQL
      row = dbi.select_one(sql, odba_id, key_dump)
      row.first unless row.nil?
    end

    def collection_remove(odba_id, key_dump)
      dbi.do <<-SQL, odba_id, key_dump
        DELETE FROM collection
        WHERE odba_id = ? AND key = ?
      SQL
    end

    def collection_store(odba_id, key_dump, value_dump)
      dbi.do <<-SQL, odba_id, key_dump, value_dump
        INSERT INTO collection (odba_id, key, value)
        VALUES (?, ?, ?)
      SQL
    end

    def condition_index_delete(index_name, origin_id,
      search_terms, target_id = nil)
      values = []
      sql = "DELETE FROM #{index_name}"
      sql << if origin_id
        " WHERE origin_id = ?"
      else
        " WHERE origin_id IS ?"
      end
      search_terms.each { |key, value|
        sql << " AND %s = ?" % key
        values << value
      }
      if target_id
        sql << " AND target_id = ?"
        values << target_id
      end
      dbi.do sql, origin_id, *values
    end

    def condition_index_ids(index_name, id, id_name)
      sql = <<-SQL
        SELECT DISTINCT *
        FROM #{index_name}
        WHERE #{id_name}=?
      SQL
      dbi.select_all(sql, id)
    end

    def create_dictionary_map(language)
      dbi.do <<-SQL
        ALTER TEXT SEARCH CONFIGURATION default_#{language}
        ALTER MAPPING FOR
          asciiword, asciihword, hword_asciipart,
          word, hword, hword_part, hword_numpart,
          numword, numhword
        WITH #{language}_ispell, #{language}_stem;
      SQL
      dbi.do <<-SQL
        ALTER TEXT SEARCH CONFIGURATION default_#{language}
        ALTER MAPPING FOR
          host, file, int, uint, version
        WITH simple;
      SQL
      # drop from default setting
      dbi.do <<-SQL
      ALTER TEXT SEARCH CONFIGURATION default_#{language}
          DROP MAPPING FOR
          email, url, url_path, sfloat, float
      SQL
    end

    def create_condition_index(table_name, definition)
      dbi.do <<~SQL
        CREATE TABLE IF NOT EXISTS #{table_name} (
          origin_id INTEGER,
          #{definition.collect { |*pair| pair.join(" ") }.join(",\n  ")},
          target_id INTEGER
        );
      SQL
      # index origin_id
      dbi.do <<~SQL
        CREATE INDEX IF NOT EXISTS origin_id_#{table_name} ON #{table_name}(origin_id);
      SQL
      # index search_term
      definition.each { |name, datatype|
        dbi.do <<~SQL
          CREATE INDEX IF NOT EXISTS #{name}_#{table_name} ON #{table_name}(#{name});
        SQL
      }
      # index target_id
      dbi.do <<~SQL
        CREATE INDEX IF NOT EXISTS target_id_#{table_name} ON #{table_name}(target_id);
      SQL
    end

    def create_fulltext_index(table_name)
      dbi.do <<~SQL
        DROP TABLE IF EXISTS #{table_name};
      SQL
      dbi.do <<~SQL
        CREATE TABLE IF NOT EXISTS #{table_name}  (
          origin_id INTEGER,
          search_term tsvector,
          target_id INTEGER
        ) ;
      SQL
      # index origin_id
      dbi.do <<~SQL
        CREATE INDEX IF NOT EXISTS origin_id_#{table_name} ON #{table_name}(origin_id);
      SQL
      dbi.do <<~SQL
        CREATE INDEX IF NOT EXISTS search_term_#{table_name}
        ON #{table_name} USING gist(search_term);
      SQL
      # index target_id
      dbi.do <<~SQL
        CREATE INDEX IF NOT EXISTS target_id_#{table_name} ON #{table_name}(target_id);
      SQL
    end

    def create_index(table_name)
      dbi.do <<-SQL
        DROP TABLE IF EXISTS #{table_name};
      SQL
      dbi.do <<-SQL
        CREATE TABLE IF NOT EXISTS #{table_name} (
          origin_id INTEGER,
          search_term TEXT,
          target_id INTEGER
        ) ;
      SQL
      # index origin_id
      dbi.do <<-SQL
        CREATE INDEX IF NOT EXISTS origin_id_#{table_name}
        ON #{table_name}(origin_id)
      SQL
      # index search_term
      dbi.do <<-SQL
        CREATE INDEX IF NOT EXISTS search_term_#{table_name}
        ON #{table_name}(search_term)
      SQL
      # index target_id
      dbi.do <<-SQL
        CREATE INDEX IF NOT EXISTS target_id_#{table_name}
        ON #{table_name}(target_id)
      SQL
    end

    def dbi
      Thread.current[:txn] || @dbi
    end

    def drop_index(index_name)
      dbi.do "DROP TABLE IF EXISTS #{index_name}"
    end

    def delete_index_element(index_name, odba_id, id_name)
      dbi.do <<-SQL, odba_id
        DELETE FROM #{index_name} WHERE #{id_name} = ?
      SQL
    end

    def delete_persistable(odba_id)
      # delete origin from connections
      dbi.do <<-SQL, odba_id
				DELETE FROM object_connection WHERE origin_id = ?
      SQL
      # delete target from connections
      dbi.do <<-SQL, odba_id
				DELETE FROM object_connection WHERE target_id = ?
      SQL
      # delete from collections
      dbi.do <<-SQL, odba_id
				DELETE FROM collection WHERE odba_id = ?
      SQL
      # delete from objects
      dbi.do <<-SQL, odba_id
				DELETE FROM object WHERE odba_id = ?
      SQL
    end

    def ensure_object_connections(origin_id, target_ids)
      sql = <<-SQL
        SELECT target_id FROM object_connection
        WHERE origin_id = ?
      SQL
      target_ids.uniq!
      update_ids = target_ids
      ## use self.dbi instead of @dbi to get information about
      ## object_connections previously stored within this transaction
      if (rows = dbi.select_all(sql, origin_id))
        old_ids = rows.collect { |row| row[0] }
        old_ids.uniq!
        delete_ids = old_ids - target_ids
        update_ids = target_ids - old_ids
        unless delete_ids.empty?
          until (ids = delete_ids.slice!(0, BULK_FETCH_STEP)).empty?
            dbi.do <<-SQL, origin_id
              DELETE FROM object_connection
              WHERE origin_id = ? AND target_id IN (#{ids.join(",")})
            SQL
          end
        end
      end
      sth = dbi.prepare <<-SQL
        INSERT INTO object_connection (origin_id, target_id)
        VALUES (?, ?)
      SQL
      update_ids.each { |id|
        sth.execute(origin_id, id)
      }
      sth.finish
    end

    def ensure_target_id_index(table_name)
      # index target_id
      dbi.do <<-SQL
        CREATE INDEX IF NOT EXISTS target_id_#{table_name}
        ON #{table_name}(target_id)
      SQL
    rescue
    end

    def extent_count(klass)
      dbi.select_one(<<-EOQ, klass.to_s).first
        SELECT COUNT(odba_id) FROM object WHERE extent = ?
      EOQ
    end

    def extent_ids(klass)
      dbi.select_all(<<-EOQ, klass.to_s).flatten
        SELECT odba_id FROM object WHERE extent = ?
      EOQ
    end

    def fulltext_index_delete(index_name, id, id_name)
      dbi.do <<-SQL, id
        DELETE FROM #{index_name}
        WHERE #{id_name} = ?
      SQL
    end

    def get_server_version
      /\s([\d\.]+)\s/.match(dbi.select_all("select version();").first.first)[1]
    end

    def fulltext_index_target_ids(index_name, origin_id)
      sql = <<-SQL
        SELECT DISTINCT target_id
        FROM #{index_name}
        WHERE origin_id=?
      SQL
      dbi.select_all(sql, origin_id)
    end

    def generate_dictionary(language)
      # postgres searches for the dictionary file in the directory share/tsearch_data of it installation location
      # By default under gentoo, this is /usr/share/postgresql/tsearch_data/
      # Use /usr/local/pgsql-10.1/bin/pg_config --sharedir to get the current value
      # As we have no way to get the current installation path, we do not check whether the files are present or not
      file = "fulltext"
      # setup configuration
      dbi.do <<-SQL
        DROP TEXT SEARCH DICTIONARY IF EXISTS  public.default_#{language};
      SQL
      dbi.do <<-SQL
        CREATE TEXT SEARCH CONFIGURATION public.default_#{language} ( COPY = pg_catalog.#{language} );
      SQL
      # ispell
      dbi.do <<-SQL
        DROP TEXT SEARCH DICTIONARY IF EXISTS  #{language}_ispell;
      SQL
      dbi.do <<-SQL
        CREATE TEXT SEARCH DICTIONARY #{language}_ispell (
          TEMPLATE  = ispell,
          DictFile  = #{language}_#{file},
          AffFile   = #{language}_#{file},
          StopWords = #{language}_#{file}
        );
      SQL
      # stem is already there.
      create_dictionary_map(language)
    end

    def index_delete_origin(index_name, odba_id, term)
      dbi.do <<-SQL, odba_id, term
        DELETE FROM #{index_name} 
        WHERE origin_id = ?
        AND search_term = ?
      SQL
    end

    def index_delete_target(index_name, origin_id, search_term, target_id)
      dbi.do <<-SQL, origin_id, search_term, target_id
        DELETE FROM #{index_name} 
        WHERE origin_id = ?
        AND search_term = ?
        AND target_id = ?
      SQL
    end

    def index_fetch_keys(index_name, length = nil)
      expr = if length
        "substr(search_term, 1, #{length})"
      else
        "search_term"
      end
      sql = <<-SQL
        SELECT DISTINCT #{expr} AS key
        FROM #{index_name}
        ORDER BY key
      SQL
      dbi.select_all(sql).flatten
    end

    def index_matches(index_name, substring, limit = nil, offset = 0)
      sql = <<-SQL
        SELECT DISTINCT search_term AS key
        FROM #{index_name}
        WHERE search_term LIKE ?
        ORDER BY key
      SQL
      if limit
        sql << "LIMIT #{limit}\n"
      end
      if offset > 0
        sql << "OFFSET #{offset}\n"
      end
      dbi.select_all(sql, substring + "%").flatten
    end

    def index_origin_ids(index_name, target_id)
      sql = <<-SQL
        SELECT DISTINCT origin_id, search_term
        FROM #{index_name}
        WHERE target_id=?
      SQL
      dbi.select_all(sql, target_id)
    end

    def index_target_ids(index_name, origin_id)
      sql = <<-SQL
        SELECT DISTINCT target_id, search_term
        FROM #{index_name}
        WHERE origin_id=?
      SQL
      dbi.select_all(sql, origin_id)
    end

    def max_id
      @id_mutex.synchronize do
        ensure_next_id_set
        @next_id
      end
    end

    def next_id
      @id_mutex.synchronize do
        ensure_next_id_set
        @next_id += 1
      end
    end

    def update_max_id(id)
      @id_mutex.synchronize do
        @next_id = id
      end
    end

    def reserve_next_id(reserved_id)
      @id_mutex.synchronize do
        ensure_next_id_set
        if @next_id < reserved_id
          @next_id = reserved_id
        else
          raise OdbaDuplicateIdError,
            "The id '#{reserved_id}' has already been assigned"
        end
      end
    end

    def remove_dictionary(language)
      # remove configuration
      dbi.do <<-SQL
        DROP TEXT SEARCH CONFIGURATION IF EXISTS default_#{language}
      SQL
      # remove ispell dictionaries
      dbi.do <<-SQL
        DROP TEXT SEARCH DICTIONARY IF EXISTS #{language}_ispell;
      SQL
    end

    def restore(odba_id)
      row = dbi.select_one("SELECT content FROM object WHERE odba_id = ?", odba_id)
      row.first unless row.nil?
    end

    def retrieve_connected_objects(target_id)
      sql = <<-SQL
				SELECT origin_id FROM object_connection 
				WHERE target_id = ?
      SQL
      dbi.select_all(sql, target_id)
    end

    def retrieve_from_condition_index(index_name, conditions, limit = nil)
      sql = <<-EOQ
        SELECT target_id, COUNT(target_id) AS relevance
        FROM #{index_name}
        WHERE TRUE
      EOQ
      values = []
      conditions.collect { |name, info|
        val = nil
        condition = nil
        if info.is_a?(Hash)
          condition = info["condition"]
          if (val = info["value"])
            if /i?like/i.match?(condition)
              val += "%"
            end
            condition = "#{condition || "="} ?"
            values.push(val.to_s)
          end
        elsif info
          condition = "= ?"
          values.push(info.to_s)
        end
        sql << <<-EOQ
          AND #{name} #{condition || "IS NULL"}
        EOQ
      }
      sql << "        GROUP BY target_id\n"
      if limit
        sql << " LIMIT #{limit}"
      end
      dbi.select_all(sql, *values)
    end

    def retrieve_from_fulltext_index(index_name, search_term, limit = nil)
      ## this combination of gsub statements solves the problem of
      #  properly escaping strings of this form: "(2:1)" into
      #  '\(2\:1\)' (see test_retrieve_from_fulltext_index)
      term = search_term.strip.gsub(/\s+/, "&").squeeze("&")
        .gsub(/[():]/i, '\\ \\&').gsub(/\s/, "")
      sql = <<-EOQ
				SELECT target_id, 
					max(ts_rank(search_term, to_tsquery(?))) AS relevance
				FROM #{index_name} 
				WHERE search_term @@ to_tsquery(?)
				GROUP BY target_id
				ORDER BY relevance DESC
      EOQ
      if limit
        sql << " LIMIT #{limit}"
      end
      dbi.select_all(sql, term, term)
    rescue DBI::ProgrammingError => e
      warn("ODBA::Storage.retrieve_from_fulltext_index rescued a DBI::ProgrammingError(#{e.message}). Query:")
      warn("self.dbi.select_all(#{sql}, #{term}, #{term})")
      warn("returning empty result")
      []
    end

    def retrieve_from_index(index_name, search_term,
      exact = nil, limit = nil)
      unless exact
        search_term += "%"
      end
      sql = <<-EOQ
        SELECT target_id, COUNT(target_id) AS relevance
        FROM #{index_name}
        WHERE search_term LIKE ?
        GROUP BY target_id
      EOQ
      if limit
        sql << " LIMIT #{limit}"
      end
      dbi.select_all(sql, search_term)
    end

    def restore_collection(odba_id)
      dbi.select_all <<-EOQ
				SELECT key, value FROM collection WHERE odba_id = #{odba_id}
      EOQ
    end

    def restore_named(name)
      row = dbi.select_one("SELECT content FROM object WHERE name = ?",
        name)
      row.first unless row.nil?
    end

    def restore_prefetchable
      dbi.select_all <<-EOQ
				SELECT odba_id, content FROM object WHERE prefetchable = true
      EOQ
    end

    def setup
      old_stderr = $stderr.dup # We want to silence the annoying NOTICE:  relation "object" already exists, skipping
      $stderr.reopen("/dev/null", "w")
      TABLES.each { |name, definition|
        begin
          dbi.do(definition)
          $stdout.sync
        rescue DBI::ProgrammingError
          $stderr = old_stderr
        end
      }
      unless dbi.columns("object").any? { |col| col.name == "extent" }
        dbi.do <<~EOS
          ALTER TABLE object ADD COLUMN extent TEXT;
          CREATE INDEX IF NOT EXISTS extent_index ON object(extent);
        EOS
      end
      $stderr = old_stderr
    end

    def store(odba_id, dump, name, prefetchable, klass)
      sql = "SELECT name FROM object WHERE odba_id = ?"
      if (row = dbi.select_one(sql, odba_id))
        name ||= row["name"]
        dbi.do <<-SQL, dump, name, prefetchable, klass.to_s, odba_id
					UPDATE object SET 
					content = ?,
					name = ?,
					prefetchable = ?,
          extent = ?
					WHERE odba_id = ?
        SQL
      else
        dbi.do <<-SQL, odba_id, dump, name, prefetchable, klass.to_s
					INSERT INTO object (odba_id, content, name, prefetchable, extent)
					VALUES (?, ?, ?, ?, ?)
        SQL
      end
    end

    def transaction(&block)
      retval = nil
      @dbi.transaction { |dbi|
        ## this should not be necessary anymore:
        # dbi['AutoCommit'] = false
        Thread.current[:txn] = dbi
        retval = block.call
      }
      retval
    ensure
      ## this should not be necessary anymore:
      # dbi['AutoCommit'] = true
      Thread.current[:txn] = nil
    end

    def update_condition_index(index_name, origin_id, search_terms, target_id)
      keys = []
      vals = []
      search_terms.each { |key, val|
        keys.push(key)
        vals.push(val)
      }
      if target_id
        dbi.do <<~SQL, origin_id, target_id, *vals
          INSERT INTO #{index_name} (origin_id, target_id, #{keys.join(", ")})
          VALUES (?, ?#{", ?" * keys.size})
        SQL
      else
        key_str = keys.collect { |key| "#{key}=?" }.join(", ")
        dbi.do <<~SQL, *vals.push(origin_id)
          UPDATE #{index_name} SET #{key_str}
          WHERE origin_id = ?
        SQL
      end
    end

    def update_fulltext_index(index_name, origin_id, search_term, target_id)
      search_term = search_term.gsub(/\s+/, " ").strip
      if target_id
        dbi.do <<~SQL, origin_id.to_s, search_term, target_id
          INSERT INTO #{index_name} (origin_id, search_term, target_id)
          VALUES (?, to_tsvector(?), ?)
        SQL
      else
        dbi.do <<~SQL, search_term, origin_id
          UPDATE #{index_name} SET search_term=to_tsvector(?)
          WHERE origin_id=?
        SQL
      end
    end

    def update_index(index_name, origin_id, search_term, target_id)
      if target_id
        dbi.do <<-SQL, origin_id, search_term, target_id
          INSERT INTO #{index_name} (origin_id, search_term, target_id) 
          VALUES (?, ?, ?)
        SQL
      else
        dbi.do <<-SQL, search_term, origin_id
          UPDATE #{index_name} SET search_term=?
          WHERE origin_id=?
        SQL
      end
    end

    private

    def ensure_next_id_set
      if @next_id.nil?
        @next_id = restore_max_id
      end
    end

    def restore_max_id
      row = dbi.select_one("SELECT odba_id FROM object ORDER BY odba_id DESC LIMIT 1")
      if row.nil? || row.first.nil?
        0
      else
        row.first
      end
    end
  end
end
