<?php

namespace Globalis\MysqlDataAnonymizer;

use Amp;
use Amp\Promise;
use Amp\Mysql;
use Exception;

class Anonymizer
{
    /**
     * Database interactions object.
     *
     * @var DatabaseInterface
     */
    protected $mysql_pool;

    /**
     * Generator object (e.g \Faker\Factory).
     *
     * @var mixed
     */
    protected $generator;

    /**
     * Configuration array.
     *
     * @var array
     */
    protected $config = [];

    /**
     * Blueprints for tables.
     *
     * @var array
     */
    protected $blueprints = [];

    /**
     * Constructor.
     *
     * @param mixed             $generator
     */
    public function __construct($generator = null)
    {
    	$this->config = require __DIR__ . "/../config/config.php";
        $this->mysql_pool = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".$this->config['DB_HOST'].";user=".$this->config['DB_USER'].";pass=".$this->config['DB_PASSWORD'].";db=". $this->config['DB_NAME']), $this->config['NB_MAX_MYSQL_CLIENT'] ?? 20);

        if (is_null($generator) && class_exists('\Faker\Factory')) {
            $generator = \Faker\Factory::create($this->config['DEFAULT_GENERATOR_LOCALE'] ?? 'en_US');
        }

        if (!is_null($generator)) {
            $this->setGenerator($generator);
        }
    }

    /**
     * Setter for generator.
     *
     * @param mixed $generator
     *
     * @return $this
     */
    public function setGenerator($generator)
    {
        $this->generator = $generator;

        return $this;
    }

    /**
     * Getter for generator.
     *
     * @return mixed
     */
    public function getGenerator()
    {
        return $this->generator;
    }

    /**
     * Perform data anonymization.
     *
     * @return void
     */
    public function run()
    {
        Amp\Loop::run(function () {
            $promises = [];
            $promise_count = 0;
            foreach ($this->blueprints as $table => $blueprint) {
                $columns = implode(',', array_merge($blueprint->primary, array_column($blueprint->columns, 'name')));
                $sql = "SELECT {$columns} FROM {$table}";

                if(!empty($blueprint->globalWhere)) {
                    $sql .= " WHERE " . implode(" AND ", $blueprint->globalWhere);
                }

                $result = yield $this->mysql_pool->query($sql);
                $rowNum = 0;

                while (yield $result->advance()) {

                    $row = $result->getCurrent();
                    $promises[] = $this->updateByPrimary(
                        $blueprint->table,
                        Helpers::arrayOnly($row, $blueprint->primary),
                        $blueprint->columns,
                        $rowNum);
                    $rowNum ++;
                    $promise_count ++;

                    if($promise_count === $this->config['NB_MAX_PROMISE_IN_LOOP']) {
                        yield \Amp\Promise\all($promises);
                        $promises = [];
                        $promise_count = 0;
                    }
                }
            }
        });
    }

    /**
     * Describe a table with a given callback.
     *
     * @param string   $name
     * @param callable $callback
     *
     * @return void
     */
    public function table($name, callable $callback)
    {
        $blueprint = new Blueprint($name, $callback);

        $this->blueprints[$name] = $blueprint->build();
    }


    /**
     * Calculate new value for each row.
     *
     * @param string|callable $replace
     * @param int             $rowNum
     *
     * @return string
     */
    protected function calculateNewValue($replace, $rowNum)
    {
        $value = $this->handlePossibleClosure($replace);

        return addslashes($this->replacePlaceholders($value, $rowNum));
    }

    /**
     * Replace placeholders.
     *
     * @param mixed $value
     * @param int   $rowNum
     *
     * @return mixed
     */
    protected function replacePlaceholders($value, $rowNum)
    {
        if (!is_string($value)) {
            return $value;
        }

        return str_replace('#row#', $rowNum, $value);
    }

    /**
     * @param $replace
     *
     * @return mixed
     */
    protected function handlePossibleClosure($replace)
    {
        if (!is_callable($replace)) {
            return $replace;
        }

        if ($this->generator === null) {
            throw new Exception('You forgot to set a generator');
        }

        return call_user_func($replace, $this->generator);
    }

    public function updateByPrimary($table, $primaryKeyValue, $columns, $rowNum)
    {
        $where = $this->buildWhereForArray($primaryKeyValue);

        $set = $this->buildSetForArray($columns, $rowNum);

        $sql = "UPDATE
                    {$table}
                SET
                    {$set}
                WHERE
                    {$where}";

        return $this->mysql_pool->query($sql);
    }

     /**
     * Build SQL where for key-value array.
     *
     * @param array $primaryKeyValue
     *
     * @return string
     */
    protected function buildWhereForArray($primaryKeyValue)
    {
        $where = [];
        foreach ($primaryKeyValue as $key => $value) {
            $where[] = "{$key}='{$value}'";
        }

        return implode(' AND ', $where);
    }

    /**
     * Build SQL where for key-value array.
     *
     * @param array $primaryKeyValue
     *
     * @return string
     */
    protected function buildSetForArray($columns, $rowNum)
    {
        $set = [];
        foreach ($columns as $column) {
            $newData = $this->calculateNewValue($column['replace'], $rowNum);

            if (empty($column['where'])) {
                $set[] = "{$column['name']}='{$newData}'";
            } else {
                $set[] = "{$column['name']}=(
                    CASE 
                      WHEN {$column['where']} THEN '{$newData}'
                      ELSE {$column['name']}
                    END)";
            }
        }
        return implode(' ,', $set);
    }
}
