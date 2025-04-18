#!/usr/bin/env ruby

# ODBA::Storage -- odba -- 08.12.2011 -- mhatakeyama@ywesee.com
# ODBA::Storage -- odba -- 29.04.2004 -- hwyss@ywesee.com rwaltert@ywesee.com mwalder@ywesee.com

require "debug" if defined?(Test::Unit::TestCase)
require 'singleton'
require 'sequel'

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
          res = dbi[sql]
          rows = res.collect{|x| [x[:odba_id], x[:content]]}
        end
        rows
      end
    end

    def collection_fetch(odba_id, key_dump)
      sql = %(SELECT value FROM collection WHERE odba_id = ? AND key = ?)
      res = dbi[sql, odba_id, key_dump]
      rows = res.collect{|x| [x[:value]]}
      rows.first&.first unless rows.nil?
    end

    def collection_remove(odba_id, key_dump)
      sql = %(DELETE FROM collection WHERE odba_id = ? AND key = ?)
      res = self.dbi[sql, odba_id, key_dump].delete
    end

    def collection_store(odba_id, key_dump, value_dump)
      sql = %(INSERT INTO collection (odba_id, key, value) VALUES (?, ?, ?))
      self.dbi[sql, odba_id, key_dump, value_dump].insert
    end

    def condition_index_delete(index_name, origin_id,
      search_terms, target_id = nil)
      # Niklaus believes in April 2025, that origin_id must always be a valid integer
      raise OdbaError unless origin_id
      values = []
      sql = "DELETE FROM #{index_name}"
      sql << " WHERE origin_id = ?"
      search_terms.each { |key, value|
        sql << " AND %s = ?" % key
        values << value.to_s
      }
      if target_id
        sql << " AND target_id = ?"
        values << target_id
      end
      self.dbi[sql, origin_id, *values].delete
    end

    def condition_index_ids(index_name, id, id_name)
      sql = %(SELECT DISTINCT * FROM #{index_name} WHERE #{id_name}=?)
      res = dbi[sql, id]
      rows = res.collect{|x| x.values}
    end

    def create_dictionary_map(language)
      sql = %(ALTER TEXT SEARCH CONFIGURATION default_#{language}
ALTER MAPPING FOR host, file, int, uint, version
WITH simple;)
    self.dbi.run sql

    # drop from default setting
    sql =%(ALTER TEXT SEARCH CONFIGURATION default_#{language}
DROP MAPPING FOR email, url, url_path, sfloat, float)
    self.dbi.run sql

    sql = %(ALTER TEXT SEARCH CONFIGURATION default_#{language}
ALTER MAPPING FOR
  asciiword, asciihword, hword_asciipart,
  word, hword, hword_part, hword_numpart,
  numword, numhword
WITH #{language}_ispell, #{language}_stem;)
    self.dbi.run sql

    end

    def create_condition_index(table_name, definition)
      sql = %(CREATE TABLE IF NOT EXISTS #{table_name} (
  origin_id INTEGER,
  #{definition.collect { |*pair| pair.join(' ') }.join(",\n  ") },
  target_id INTEGER
);)
    self.dbi.run sql
      #index origin_id
    sql = %(CREATE INDEX IF NOT EXISTS origin_id_#{table_name} ON #{table_name}(origin_id);)
      # index search_term
      definition.each { |name, datatype|
        sql = %(CREATE INDEX IF NOT EXISTS #{name}_#{table_name} ON #{table_name}(#{name});)
        self.dbi.run sql
      }
      #index target_id
      sql = %(CREATE INDEX IF NOT EXISTS target_id_#{table_name} ON #{table_name}(target_id);)
      self.dbi.run sql
    end

    def create_fulltext_index(table_name)
      sql = %(DROP TABLE IF EXISTS #{table_name};)
      self.dbi.run sql

      sql = %(CREATE TABLE IF NOT EXISTS #{table_name}  (
  origin_id INTEGER,
  search_term tsvector,
  target_id INTEGER
);)
      self.dbi.run sql
      #index origin_id
      sql = %(CREATE INDEX IF NOT EXISTS origin_id_#{table_name} ON #{table_name}(origin_id);)
      self.dbi.run sql
      sql = %(CREATE INDEX IF NOT EXISTS search_term_#{table_name}
ON #{table_name} USING gist(search_term);)
      self.dbi.run sql
      #index target_id
      sql = %(CREATE INDEX IF NOT EXISTS target_id_#{table_name} ON #{table_name}(target_id);)
      self.dbi.run sql
    end
    def create_index(table_name)
      sql = %(DROP TABLE IF EXISTS #{table_name};)
      self.dbi.run sql
      sql = %(CREATE TABLE IF NOT EXISTS #{table_name} (
origin_id INTEGER, search_term TEXT, target_id INTEGER) ;)
      self.dbi.run sql
      #index origin_id
      sql = %(CREATE INDEX IF NOT EXISTS origin_id_#{table_name}
        ON #{table_name}(origin_id))
      self.dbi.run sql
      #index search_term
      sql = %(CREATE INDEX IF NOT EXISTS search_term_#{table_name}
        ON #{table_name}(search_term))
      self.dbi.run sql
      #index target_id
      sql = %(CREATE INDEX IF NOT EXISTS target_id_#{table_name}
        ON #{table_name}(target_id))
      self.dbi.run sql
    end
		def dbi
			Thread.current[:txn] || @dbi
		end
		def drop_index(index_name)
			self.dbi.run "DROP TABLE IF EXISTS #{index_name}"
		end
    def delete_index_element(index_name, odba_id, id_name)
      sql = %(DELETE FROM #{index_name} WHERE #{id_name} = ?)
      self.dbi[sql, odba_id].delete
    end

    def delete_persistable(odba_id)
      # delete origin from connections
      dbi["DELETE FROM object_connection WHERE origin_id = ?", odba_id].delete
      # delete target from connections
      dbi["DELETE FROM object_connection WHERE target_id = ?", odba_id].delete
      # delete from collections
      dbi["DELETE FROM collection WHERE odba_id = ?", odba_id].delete
      # delete from objects
      dbi["DELETE FROM object WHERE odba_id = ?", odba_id].delete
    end

    def ensure_object_connections(origin_id, target_ids)
      sql = %(SELECT target_id FROM object_connection  WHERE origin_id = ?)
      target_ids.uniq!
      update_ids = target_ids
      ## use self.dbi instead of @dbi to get information about
      ## object_connections previously stored within this transaction
      rows = dbi[sql, origin_id]
      if (rows =  dbi[sql, origin_id])
        old_ids = rows.collect { |row| row.values}.flatten
        old_ids.uniq!
        delete_ids = old_ids - target_ids
        update_ids = target_ids - old_ids
        unless(delete_ids.empty?)
          while(!(ids = delete_ids.slice!(0, BULK_FETCH_STEP)).empty?)
            sql = %(DELETE FROM object_connection WHERE origin_id = ? AND target_id IN (#{ids.join(",")}))
            self.dbi[sql, origin_id].delete
          end
        end
      end
      update_ids.each do |id|
        sql = %(INSERT INTO object_connection (origin_id, target_id) VALUES (?, ?))
        self.dbi[sql, origin_id, id].insert
      end
    end

    def ensure_target_id_index(table_name)
      return nil unless @dbi.table_exists?(table_name)
      #index target_id
      sql = %(CREATE INDEX IF NOT EXISTS target_id_#{table_name} ON #{table_name}(target_id))
      self.dbi[sql]
      if @dbi[table_name.to_sym].columns.count{|x| /target_id/.match(x.to_s)}
        0
      else
        nil
      end
    end

    def extent_count(klass)
      sql = %(SELECT COUNT(odba_id) FROM object WHERE extent = ?)
      self.dbi[sql, klass.to_s].first[:count]
    end

    def extent_ids(klass)
      sql = "SELECT odba_id FROM object WHERE extent = ?"
      res = dbi[sql, klass]
      all = res.collect{|x| x[:odba_id]}
    end

    def fulltext_index_delete(index_name, id, id_name)
      sql = %(DELETE FROM #{index_name} WHERE #{id_name} = ?)
      dbi[sql, id].delete
    end

    def get_server_version
      if defined?(:server_version) # for a postgres
        version = @dbi.server_version
      elsif defined?(:sqlite_version) # for a sqlite3
        version = @dbi.sqlite_version
      else
        version = -1
      end
    end

    def fulltext_index_target_ids(index_name, origin_id)
      sql = %(SELECT DISTINCT target_id FROM #{index_name} WHERE origin_id=?)
      res = dbi[sql, origin_id].collect {|x| x.values}
    end

    def generate_dictionary(language)
      # postgres searches for the dictionary file in the directory share/tsearch_data of it installation location
      # By default under gentoo, this is /usr/share/postgresql/tsearch_data/
      # Use /usr/local/pgsql-10.1/bin/pg_config --sharedir to get the current value
      # As we have no way to get the current installation path, we do not check whether the files are present or not
      file = "fulltext"
      # setup configuration
      sql = %(DROP TEXT SEARCH DICTIONARY IF EXISTS  public.default_#{language};)
      self.dbi.run sql
      sql = %(CREATE TEXT SEARCH CONFIGURATION public.default_#{language} ( COPY = pg_catalog.#{language} );)
      self.dbi.run sql
      # ispell
      sql = %(DROP TEXT SEARCH DICTIONARY IF EXISTS  #{language}_ispell;)
      self.dbi.run sql
      sql = %(CREATE TEXT SEARCH DICTIONARY #{language}_ispell (
TEMPLATE  = ispell, DictFile  = #{language}_#{file}, AffFile   = #{language}_#{file}, StopWords = #{language}_#{file}
);)
      self.dbi.run sql
      # stem is already there.
      create_dictionary_map(language)
    end

    def index_delete_origin(index_name, odba_id, term)
      sql = %(DELETE FROM #{index_name} WHERE origin_id = ? AND search_term = ?)
      self.dbi[sql, odba_id, term].delete
    end

    def index_delete_target(index_name, origin_id, search_term, target_id)
      sql = %(DELETE FROM #{index_name} WHERE origin_id = ? AND search_term = ? AND target_id = ?)
      self.dbi[sql, origin_id, search_term, target_id].delete
    end

    def index_fetch_keys(index_name, length = nil)
      expr = if length
        "substr(search_term, 1, #{length})"
      else
        "search_term"
      end
      sql = %(SELECT DISTINCT #{expr} AS key FROM #{index_name} ORDER BY key)
      res = dbi[sql]
      res.collect{|x| x.values}.flatten
    end

    def index_matches(index_name, substring, limit = nil, offset = 0)
      sql = %(SELECT DISTINCT search_term AS key FROM #{index_name} WHERE search_term LIKE ? ORDER BY key )
      if limit
        sql << "LIMIT #{limit}\n"
      end
      if offset > 0
        sql << "OFFSET #{offset}\n"
      end
      res = dbi[sql, substring + "%"]
      res.collect{|x| x.values}.flatten
    end

    def index_origin_ids(index_name, target_id)
      sql = %(SELECT DISTINCT origin_id, search_term FROM #{index_name} WHERE target_id=?)
      res = dbi[sql, target_id]
      res.collect{|x| x.values}
    end

    def index_target_ids(index_name, origin_id)
      sql = %(SELECT DISTINCT target_id, search_term FROM #{index_name} WHERE origin_id=?)
      res = dbi[sql, origin_id]
      res.collect{|x| x.values}
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
      sql = %(DROP TEXT SEARCH CONFIGURATION IF EXISTS default_#{language};)
      self.dbi.run sql
      # remove ispell dictionaries
      sql = %(DROP TEXT SEARCH DICTIONARY IF EXISTS #{language}_ispell;)
      self.dbi.run sql
    end

    def restore(odba_id)
      sql = %(SELECT content FROM object WHERE odba_id = ?)
      rows = dbi[sql, odba_id]
      rows.first[:content] if rows.first
    end

    def retrieve_connected_objects(target_id)
      sql = %(SELECT origin_id FROM object_connection WHERE target_id = ?)
      res = dbi[sql, target_id]
      res.collect{|x| x.values}
    end

    def retrieve_from_condition_index(index_name, conditions, limit = nil)
      sql = %(SELECT target_id, COUNT(target_id) AS relevance FROM #{index_name} WHERE TRUE)
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
        sql << %( AND #{name} #{condition || "IS NULL"})
      }
      sql << " GROUP BY target_id\n"
      if limit
        sql << " LIMIT #{limit}"
      end
      res = dbi[sql, *values].collect{|x| x.values}
      res
    end

    def retrieve_from_fulltext_index(index_name, search_term, limit = nil)
      ## this combination of gsub statements solves the problem of
      #  properly escaping strings of this form: "(2:1)" into
      #  '\(2\:1\)' (see test_retrieve_from_fulltext_index)
      term = search_term.strip.gsub(/\s+/, "&").squeeze("&")
        .gsub(/[():]/i, '\\ \\&').gsub(/\s/, "")
      sql = %(SELECT target_id, max(ts_rank(search_term, to_tsquery(?))) AS relevance
FROM #{index_name}
WHERE search_term @@ to_tsquery(?)
GROUP BY target_id
ORDER BY relevance DESC)
      if limit
        sql << " LIMIT #{limit}"
      end
      res = dbi[sql, term, term].collect{|x| x.values}
    rescue Sequel::DatabaseError => e
      warn("ODBA::Storage.retrieve_from_fulltext_index rescued a Sequel::DatabaseError(#{e.message}). Query:")
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
      res = self.dbi[sql, search_term]
      rows = res.collect{|x| x.values}
    end

    def restore_collection(odba_id)
      sql = %(SELECT key, value FROM collection WHERE odba_id = ?;)
      res = dbi[sql, odba_id]
      rows = res.collect{|x| [x[:key], x[:value]]}
    end
    def restore_named(name)
      sql =%(SELECT content FROM object WHERE name = ?)
      res = self.dbi[sql, name]
      res.first[:content] if res.first
    end
    def restore_prefetchable
      sql =%(SELECT odba_id, content FROM object WHERE prefetchable = true)
      res = self.dbi[sql]
      res.collect{|x| x.values}
    end
    def setup
      TABLES.each { |name, definition| dbi.run(definition)}
      sql = "ALTER TABLE object ADD COLUMN extent TEXT;
CREATE INDEX IF NOT EXISTS extent_index ON object(extent);"
      unless self.dbi[:object].columns.find  { |col| col.name.match?(/extent/) }
        self.dbi.run sql
      end
    end

    def store(odba_id, dump, name, prefetchable, klass)
        sql = "SELECT name FROM object WHERE odba_id = ?"
#        if(row = self.dbi.select_one(sql, odba_id))
        if(row = self.dbi[sql, odba_id].first)
          name ||= row['name']
          sql = %(UPDATE object SET content = ?, name = ?, prefetchable = ?, extent = ? WHERE odba_id = ?)
          res = self.dbi[sql, dump, name, prefetchable, klass.to_s, odba_id].update
        else
          sql = %(INSERT INTO object (odba_id, content, name, prefetchable, extent) VALUES (?, ?, ?, ?, ?);)
          self.dbi[sql , odba_id, dump, name || "", prefetchable, klass.to_s].insert
          return 1
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
      if(target_id)
        sql = "INSERT INTO #{index_name} (origin_id, target_id, #{keys.join(', ')}) VALUES (?, ?#{', ?' * keys.size})"
        self.dbi[sql, origin_id, target_id, *vals].insert
      else
        key_str = keys.collect { |key| "#{key}=?" }.join(', ')
        sql = "UPDATE #{index_name} SET #{key_str} WHERE origin_id = ?"
        vals.push(origin_id)
        self.dbi[sql, *vals].update
      end
    end

    def update_fulltext_index(index_name, origin_id, search_term, target_id)
      search_term = search_term.gsub(/\s+/, ' ').strip
      if(target_id)
        sql = %(INSERT INTO #{index_name} (origin_id, search_term, target_id)
VALUES (?, to_tsvector(?), ?))
        self.dbi[sql, origin_id.to_s, search_term, target_id].insert
        return 1
      else
        sql = %(UPDATE #{index_name} SET search_term=to_tsvector(?) WHERE origin_id=?)
        self.dbi[sql, search_term, origin_id].update
      end
    end
    def update_index(index_name, origin_id, search_term, target_id)
      if(target_id)
        sql = %(INSERT INTO #{index_name} (origin_id, search_term, target_id) VALUES (?, ?, ?))
        self.dbi[sql, origin_id, search_term, target_id].insert
        return 1
      else
        sql = %(UPDATE #{index_name} SET search_term=? WHERE origin_id=?)
        return self.dbi[sql,  search_term, origin_id].update
      end
    end

    private

    def ensure_next_id_set
      if @next_id.nil?
        @next_id = restore_max_id
      end
    end

    def restore_max_id
      row = dbi["SELECT odba_id FROM object ORDER BY odba_id DESC LIMIT 1"]
      if row.nil? || row.first.nil?
        0
      else
        row.first[:odba_id]
      end
    end
  end
end
