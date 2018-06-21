<?php

namespace Tests\db_adapter_depended\src\ARC2\Store\Adapter;

use ARC2\Store\Adapter\CachedPDOAdapter;
use Symfony\Component\Cache\Simple\ArrayCache;

class CachedPDOAdapterTest extends PDOAdapterTest
{
    protected function getInstance()
    {
        $this->dbConfig['cache_enabled'] = true;
        $this->dbConfig['cache_instance'] = new ArrayCache();

        return new CachedPDOAdapter($this->dbConfig);
    }

    public function testFetchRow()
    {
        // create test data
        $this->fixture->simpleQuery('CREATE TABLE users (id INT(6), name VARCHAR(30) NOT NULL)');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (1, "foobar");');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (2, "foobar2");');

        $selectQuery = 'SELECT * FROM users WHERE id = 2';

        // check that cache doesnt know $selectQuery yet.
        $this->assertFalse($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));

        $user = $this->fixture->fetchRow($selectQuery);

        // check cache, that expected entry is available
        $this->assertTrue($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));

        // check if $users is equal to the cached version
        $this->assertEquals($user, $this->fixture->fetchRow($selectQuery));
    }

    public function testFetchList()
    {
        // create test data
        $this->fixture->simpleQuery('CREATE TABLE users (id INT(6), name VARCHAR(30) NOT NULL)');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (1, "foobar");');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (2, "foobar2");');

        $selectQuery = 'SELECT * FROM users';

        // check that cache doesnt know $selectQuery yet.
        $this->assertFalse($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));

        $users = $this->fixture->fetchList($selectQuery);

        // check cache, that expected entry is available
        $this->assertTrue($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));

        // check if $users is equal to the cached version
        $this->assertEquals($users, $this->fixture->fetchList($selectQuery));
    }

    // check invalidation on DELETE FROM
    public function testCacheInvalidationIfDBChangesDeleteFrom()
    {
        // create test data
        $this->fixture->simpleQuery('CREATE TABLE users (id INT(6), name VARCHAR(30) NOT NULL)');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (1, "foobar");');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (2, "foobar2");');

        $selectQuery = 'SELECT * FROM users';

        $users = $this->fixture->fetchList($selectQuery);
        $this->assertTrue($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));

        // change table and therefore the DB => invalidation of the cache
        $this->fixture->exec('DELETE FROM users WHERE id = 1');

        $this->assertFalse($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));
    }

    // check invalidation on TRUNCATE
    public function testCacheInvalidationIfDBChanges()
    {
        // create test data
        $this->fixture->simpleQuery('CREATE TABLE users (id INT(6), name VARCHAR(30) NOT NULL)');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (1, "foobar");');
        $this->fixture->simpleQuery('INSERT INTO users (id, name) VALUE (2, "foobar2");');

        $selectQuery = 'SELECT * FROM users';

        $users = $this->fixture->fetchList($selectQuery);
        $this->assertTrue($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));

        // change table and therefore the DB => invalidation of the cache
        $this->fixture->exec('TRUNCATE users');

        $this->assertFalse($this->fixture->getCacheInstance()->has(hash('sha1', $selectQuery)));
    }
}
