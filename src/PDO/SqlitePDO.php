<?php

declare(strict_types=1);

namespace Balpom\Messenger\Bridge\Sql\PDO;

use \PDO;
use \PDOException;
use \SQLite3;
use \SQLite3Exception;
use Balpom\Messenger\Bridge\Sql\Exception\PDOException as SqliteException;

class SqlitePDO implements CreatePDO
{

    static public function create(string $dsn): PDO
    {
        $dsn = self::getDatabaseFile($dsn);

        if (':memory:' !== $dsn) {
            if (!file_exists($dsn)) {
                $dir = pathinfo($dsn, PATHINFO_DIRNAME);
                if (!file_exists($dir)) {
                    mkdir($dir, 0755, true);
                }
                try {
                    $db = new SQLite3($dsn); // Create database if not exist.
                    $db->exec('PRAGMA journal_mode=WAL;'); // For multithreaded mode.
                    $db->busyTimeout(50000); // Process wait 50 seconds before fail with “SQLITE_BUSY” message.
                    $db->close();
                } catch (SQLite3Exception $exception) {
                    throw new SqliteException($exception->getMessage());
                }
            }
        }

        return self::createPDO($dsn);
    }

    static private function getDatabaseFile(string $dsn)
    {
        $dsn = trim($dsn);
        if (false === ($pos = strpos($dsn, ':'))) {
            throw new SqliteException('Incorrect DSN.');
        }

        $dsn = substr($dsn, $pos + 1);
        if (':memory:' === $dsn) {
            return $dsn;
        }

        if (empty($dsn) || '/' === $dsn) {
            throw new SqliteException('Empty database file name.');
        }

        return $dsn;
    }

    static private function createPDO(string $dbfile)
    {
        $username = null;
        $password = null;
        $options = null;
        //if (':memory:' === $dbfile) {
        $options = [PDO::ATTR_PERSISTENT => true];
        //}
        try {
            return new PDO('sqlite:' . $dbfile, $username, $password, $options);
        } catch (PDOException $exception) {
            throw new SqliteException($exception->getMessage());
        }
    }

    /**
     * Normalize path
     * (from https://stackoverflow.com/questions/20522605/ )
     *
     * @param   string  $path
     * @param   string  $separator
     * @return  string  normalized path
     */
    //static private function normalizePath($path, $separator = '\\/'): string
    static private function normalizePath(string $path): string
    {
        if (false !== strpos($path, '...')) {
            throw new SqliteException('Incorrect path.');
        }
        $n = 0;
        $a = explode("/", preg_replace("/\/\.\//", '/', $path));
        $b = [];
        for ($i = sizeof($a) - 1; $i >= 0; --$i) {
            if (trim($a[$i]) === "..") {
                $n++;
            } else {
                if ($n > 0) {
                    $n--;
                } else {
                    $b[] = $a[$i];
                }
            }
        }
        if (1 >= count($b)) {
            throw new SqliteException('Path is outside of the defined root.');
        }

        return implode("/", array_reverse($b));
    }
}
