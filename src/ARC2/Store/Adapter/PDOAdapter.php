<?php

/**
 * Adapter to enable usage of PDO functions.
 *
 * @author Benjamin Nowack <bnowack@semsol.com>
 * @author Konrad Abicht <konrad.abicht@pier-and-peer.com>
 * @license W3C Software License and GPL
 * @homepage <https://github.com/semsol/arc2>
 */

namespace ARC2\Store\Adapter;

/**
 * PDO Adapter - Handles database operations using PDO.
 */
class PDOAdapter extends AbstractAdapter
{
    protected $transactionCounter = 0;
    protected $serverVersion;

    public function checkRequirements()
    {
        if (false == \extension_loaded('pdo_mysql')) {
            throw new \Exception('Extension pdo_mysql is not loaded.');
        }
    }

    public function getAdapterName()
    {
        return 'pdo';
    }

    /**
     * Connect to server or storing a given connection.
     *
     * @param EasyDB $existingConnection Default is null.
     */
    public function connect($existingConnection = null)
    {
        // reuse a given existing connection.
        // it assumes that $existingConnection is a PDO connection object
        if (null !== $existingConnection) {
            $this->db = $existingConnection;

        // create your own connection
        } elseif (false === $this->db instanceof \PDO) {
            /*
             * build connection string
             *
             * - db_pdo_protocol: Protocol to determine server, e.g. mysql
             */
            if (false == isset($this->configuration['db_pdo_protocol'])) {
                throw new \Exception(
                    'When using PDO the protocol has to be given (e.g. mysql). Please set db_pdo_protocol in database configuration.'
                );
            }
            $dsn = $this->configuration['db_pdo_protocol'].':host='. $this->configuration['db_host'];
            if (isset($this->configuration['db_name'])) {
                $dsn .= ';dbname='.$this->configuration['db_name'];
            }

            // set charset
            $dsn .= ';charset=utf8mb4';

            $this->db = new \PDO(
                $dsn,
                $this->configuration['db_user'],
                $this->configuration['db_pwd']
            );

            $this->db->setAttribute(\PDO::ATTR_EMULATE_PREPARES, false);

            // errors DONT lead to exceptions
            // set to false for compatibility reasons with mysqli. ARC2 using mysqli does not throw any
            // exceptions, instead collects errors in a hidden array.
            $this->db->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

            // default fetch mode is associative
            $this->db->setAttribute(\PDO::ATTR_DEFAULT_FETCH_MODE, \PDO::FETCH_ASSOC);

            // from source: http://php.net/manual/de/ref.pdo-mysql.php
            // If this attribute is set to TRUE on a PDOStatement, the MySQL driver will use
            // the buffered versions of the MySQL API. But we wont rely on that, setting it false.
            $this->db->setAttribute(\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY, false);

            // This is RDF, we may need many JOINs...
            // TODO find an equivalent in other DBS
            $stmt = $this->db->prepare('SET SESSION SQL_BIG_SELECTS=1');
            $stmt->execute();
            $stmt->closeCursor();

            // with MySQL 5.6 we ran into exceptions like:
            //      PDOException: SQLSTATE[42000]: Syntax error or access violation:
            //      1140 In aggregated query without GROUP BY, expression #1 of SELECT list contains
            //      nonaggregated column 'testdb.T_0_0_0.p'; this is incompatible with sql_mode=only_full_group_by
            //
            // the following query makes this right.
            // FYI: https://stackoverflow.com/questions/23921117/disable-only-full-group-by
            $stmt = $this->db->prepare("SET sql_mode = ''");
            $stmt->execute();
            $stmt->closeCursor();
        }

        return $this->db;
    }

    /**
     * @return void
     */
    public function disconnect()
    {
        // FYI: https://stackoverflow.com/questions/18277233/pdo-closing-connection
        $this->db = null;
    }

    public function escape($value)
    {
        // quote surronds the string with ', but using trim aligns the result
        return \trim($this->db->quote($value), "'");
    }

    /**
     * @param string $sql
     *
     * @return array
     */
    public function fetchList($sql)
    {
        // save query
        $this->queries[] = [
            'query' => $sql,
            'by_function' => 'fetchList'
        ];

        if (null == $this->db) {
            $this->connect();
        }

        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $rows = $stmt->fetchAll();
        $stmt->closeCursor();

        return $rows;
    }

    public function fetchRow($sql)
    {
        // save query
        $this->queries[] = [
            'query' => $sql,
            'by_function' => 'fetchRow'
        ];

        if (null == $this->db) {
            $this->connect();
        }

        $row = false;
        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $rows = $stmt->fetchAll();
        if (0 < \count($rows)) {
            $row = \array_values($rows)[0];
        }
        $stmt->closeCursor();

        return $row;
    }

    public function getCollation()
    {
        $row = $this->fetchRow('SHOW TABLE STATUS LIKE "'.$this->getTablePrefix().'setting"');

        if (isset($row['Collation'])) {
            return $row['Collation'];
        } else {
            return '';
        }
    }

    public function getConnection()
    {
        return $this->db;
    }

    public function getConnectionId()
    {
        return $this->db->query('SELECT CONNECTION_ID()')->fetch(\PDO::FETCH_ASSOC);
    }

    /**
     * Exclusivly to PDO adapter. Returns current active driver name.
     *
     * @return string Depending on the database system, it returns something like "mysql" or "pgsql"
     */
    public function getDriverName()
    {
        return $this->db->getAttribute(\PDO::ATTR_DRIVER_NAME);
    }

    public function getDBSName()
    {
        if (null == $this->db) {
            return;
        }

        $clientVersion = \strtolower($this->db->getAttribute(\PDO::ATTR_CLIENT_VERSION));
        $serverVersion = \strtolower($this->db->getAttribute(\PDO::ATTR_SERVER_VERSION));
        if (false !== \strpos($clientVersion, 'mariadb') || false !== \strpos($serverVersion, 'mariadb')) {
            $return = 'mariadb';
        } elseif (false !== \strpos($clientVersion, 'mysql') || false !== \strpos($serverVersion, 'mysql')) {
            $return = 'mysql';
        } else {
            $return = null;
        }

        // if SERVER_VERSION gives no information, try CLIENT_VERSION
        if (null === $return) {
            $clientVersion = strtolower($this->db->getAttribute(\PDO::ATTR_CLIENT_VERSION));
            if (false !== strpos($clientVersion, 'mariadb')) {
                $return = 'mariadb';
            } elseif (false !== strpos($clientVersion, 'mysql')) {
                $return = 'mysql';
            } else {
                $return = null;
            }
        }

        return $return;
    }

    public function getServerInfo()
    {
        return $this->db->getAttribute(\constant('PDO::ATTR_CLIENT_VERSION'));
    }

    /**
     * Returns the version of the database server like 05-00-12
     */
    public function getServerVersion()
    {
        if (null == $this->serverVersion) {
            $this->serverVersion = $this->fetchRow('select version()');
            $this->serverVersion = $this->serverVersion['version()'];
        }

        return $this->serverVersion;
    }

    public function getErrorCode()
    {
        return $this->db->errorCode();
    }

    public function getErrorMessage()
    {
        return $this->db->errorInfo()[2];
    }

    public function getLastInsertId()
    {
        return $this->db->lastInsertId();
    }

    public function getNumberOfRows($sql)
    {
        // save query
        $this->queries[] = [
            'query' => $sql,
            'by_function' => 'getNumberOfRows'
        ];

        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $rowCount = \count($stmt->fetchAll());
        $stmt->closeCursor();
        return $rowCount;
    }

    /**
     * Returns a list of queries, grouped by the function they were used.
     *
     * @return array
     */
    public function getQueries()
    {
        return $this->queries;
    }

    public function getStoreName()
    {
        if (isset($this->configuration['store_name'])) {
            return $this->configuration['store_name'];
        }

        return 'arc';
    }

    public function getTablePrefix()
    {
        $prefix = '';
        if (isset($this->configuration['db_table_prefix'])) {
            $prefix = $this->configuration['db_table_prefix'].'_';
        }

        $prefix .= $this->getStoreName().'_';
        return $prefix;
    }

    /**
     * @param string $sql Query
     *
     * @return bool True if query ran fine, false otherwise.
     */
    public function simpleQuery($sql)
    {
        // save query
        $this->queries[] = [
            'query' => $sql,
            'by_function' => 'simpleQuery'
        ];

        if (false === $this->db instanceof \PDO) {
            $this->connect();
        }

        $stmt = $this->db->prepare($sql);
        $stmt->execute();
        $stmt->closeCursor();
        return true;
    }

    /**
     * Encapsulates internal PDO::exec call. This allows us to extend it, e.g. with caching functionality.
     *
     * @param string $sql
     *
     * @return int Number of affected rows.
     */
    public function exec($sql)
    {
        // save query
        $this->queries[] = [
            'query' => $sql,
            'by_function' => 'exec'
        ];

        return $this->db->exec($sql);
    }

    /*
     * Transaction related
     *
     * the following implementation prevents problems, if you have sub-transactions.
     * thanks to http://php.net/manual/de/pdo.begintransaction.php#109753
     *
     * Code also inspired by https://www.kennynet.co.uk/2008/12/02/php-pdo-nested-transactions/
     */

    /**
     * Starts a transaction, if not already started.
     *
     * If a transaction is already open, it creates a new SAVEPOINT LEVEL.
     * FYI: https://dev.mysql.com/doc/refman/5.5/en/savepoint.html
     */
    public function beginTransaction()
    {
        // if not already in a transaction
        if (false == $this->transactionsAreNestable() || false === $this->db->inTransaction()) {
            // in case one uses transactions, we set AUTO_COMMIT of the server to false to fully
            // use transaction capabilities, without it, it does not really makes sense.
            //
            // for more information: https://dev.mysql.com/doc/refman/5.5/en/innodb-autocommit-commit-rollback.html
            $this->db->exec('SET autocommit=0;');

            $this->db->beginTransaction();

        // if a transaction is already open, create a SAVEPOINT, if available
        } elseif ($this->transactionsAreNestable() && $this->db->inTransaction()) {
            // save query
            $this->queries[] = [
                'query' => "SAVEPOINT LEVEL{$this->transactionCounter}",
                'by_function' => 'beginTransaction'
            ];
            $this->db->exec("SAVEPOINT LEVEL{$this->transactionCounter}");
        }

        ++$this->transactionCounter;
    }

    public function inTransaction()
    {
        return 0 < $this->transactionCounter;
    }

    /**
     * Checks, if transactions are nestable. That is the case, if driver name is "mysql".
     *
     * @return bool True, if driver name is "mysql", because it supports nesting, false otherwise.
     */
    public function transactionsAreNestable()
    {
        return in_array($this->getDriverName(), ['mysql']);
    }

    /**
     * Commits latest changes to the DB. Careful, with DBS like MySQL datastructure changes provoke an
     * implicit COMMIT.
     *
     * If a nested transaction is to be commited, the related SAVEPOINT gets released (if feature is available).
     * If not available, nothing happens.
     */
    public function commit()
    {
        --$this->transactionCounter;

        // lowest level, therefore COMMIT changes
        if (false === $this->transactionsAreNestable() || 0 == $this->transactionCounter) {
            $this->db->commit();

        // inside a nested transaction, therefore (if available) release related SAVEPOINT
        } elseif ($this->transactionsAreNestable() && 0 < $this->transactionCounter) {
            // save query
            $this->queries[] = [
                'query' => "RELEASE SAVEPOINT LEVEL{$this->transactionCounter}",
                'by_function' => 'commit'
            ];
            $this->db->exec("RELEASE SAVEPOINT LEVEL{$this->transactionCounter}");
        }
    }

    public function rollback()
    {
        --$this->transactionCounter;

        // lowest level, therefore ROLLBACK changes
        if (false === $this->transactionsAreNestable() || 0 == $this->transactionCounter) {
            $this->db->rollback();
            // set AUTO_COMMIT behavior back to normal.
            $this->db->exec('SET autocommit=1;');

        // inside a nested transaction, therefore (if available) release related SAVEPOINT
        } elseif ($this->transactionsAreNestable() && 0 < $this->transactionCounter) {
            // save query
            $this->queries[] = [
                'query' => "ROLLBACK TO SAVEPOINT LEVEL{$this->transactionCounter}",
                'by_function' => 'rollback'
            ];
            try {
                $this->db->exec("ROLLBACK TO SAVEPOINT LEVEL{$this->transactionCounter}");
            } catch (\PDOException $e) {
                $this->rollback();
            }
        }
    }
}
