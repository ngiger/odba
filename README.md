# odba

## Branch sequel

By Niklaus Giger, Januar 2018

Exploring

* Use sequel instead of ydbi
* Remaining problems
** test/test_cache not ported
** many more errors in unit test with sqlite3
** Can sqlite3 handle multiple connections ?
** Marshalling problems when starting migel using a PostgreSQL DB
*** Adapted lib/migel/persistence/odba.rb to use

    string = 'postgres://localhost/migel?user=ydim&port=5432&password=test'
    pool = Sequel::ConnectionPool.new(db, :max_connections=>5) { Sequel.connect(string) }
    ODBA.storage.dbi = pool.db

# Overview

* https://github.com/zdavatz/odba

To see a graphical overview of the Library please see

* https://raw.githubusercontent.com/ngiger/odba/master/odba.jpeg

## DESCRIPTION:

Object Database Access - Ruby Software for ODDB.org Memory Management

## FEATURES/PROBLEMS:

* There may still be an sql bottleneck. Has to be investigated further.
* You will need postgresql installed.

## INSTALL:

* gem install odba

## DEVELOPERS:

* Masamoi Hatakeyama
* Zeno R.R. Davatz
* Hannes Wyss (up to Version 1.0)

## LICENSE:

* GPLv2.1
