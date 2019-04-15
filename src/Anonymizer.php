<?php

namespace Globalis\MysqlDataAnonymizer;

use Amp;
use Amp\Promise;
use Amp\Mysql;
use Exception;
use Globalis\MysqlDataAnonymizer\Helpers;

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
    	$this->load_config();
        $this->load_helpers();

        $this->mysql_pool = Mysql\pool(Mysql\ConnectionConfig::fromString("host=".$this->config['DB_HOST'].";user=".$this->config['DB_USER'].";pass=".$this->config['DB_PASSWORD'].";db=". $this->config['DB_NAME']), $this->config['NB_MAX_MYSQL_CLIENT'] ?? 20);

        if (is_null($generator) && class_exists('\Faker\Factory')) {
            $generator = \Faker\Factory::create($this->config['DEFAULT_GENERATOR_LOCALE'] ?? 'en_US');
        }

        if (!is_null($generator)) {
            $this->setGenerator($generator);
        }
        $this->load_providers();
    }

    protected function load_config()
    {
        $this->config = require __DIR__ . "/../config/config.php";
    }


    protected function load_helpers()
    {
        foreach (glob(__DIR__ . "/helpers/*Helper.php") as $filename)
        {
            require_once $filename;
        }
    }

    protected function load_providers()
    {
        foreach (glob(__DIR__ . "/providers/*Provider.php") as $filename)
        {
            require_once $filename;
            $className = "\\Globalis\\MysqlDataAnonymizer\\Provider\\" . basename($filename, ".php");
            if (class_exists($className)) {
                $this->generator->addProvider(new $className($this->generator));
            }
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
                $selectData = yield $this->getSelectData($table, $blueprint);
                $rowNum = 0;

                //Update every line selected
                while (yield $selectData->advance()) {
                    $row = $selectData->getCurrent();
                    $promises[] = $this->updateByPrimary(
                        $blueprint->table,
                        Helpers\GeneralHelper::arrayOnly($row, $blueprint->primary),
                        $blueprint->columns,
                        $rowNum,
                        $row);
                    $rowNum ++;
                    $promise_count ++;

                    //Wait for all the results of SQL queries and clear the promise table
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

    /**
     * Update a line by primary key given
     *
     * @param array $table
     * @param array $primaryKeyValues
     * @param array $columns
     * @param int $rowNum
     * @param array $row
     *
     * @return promise
     */
    public function updateByPrimary($table, $primaryKeyValues, $columns, $rowNum, $row)
    {
        $where = $this->buildWhereForArray($primaryKeyValues);

        $set = $this->buildSetForArray($columns, $rowNum, $row);

        $sql = "UPDATE
                    {$table}
                SET
                    {$set}
                WHERE
                    {$where}";

        return $this->mysql_pool->query($sql);
    }

    /**
     * Get lines that need to be updated
     *
     * @param array $table
     * @param Blueprint $blueprint
     *
     * @return promise
     */
    protected function getSelectData($table, $blueprint)
    {
        foreach ($blueprint->columns as $column) {
            if ($column['replaceByFields']) {
                $columns = '*';
                break;
            }
        }

        if($columns ?? false) {
            $columns = implode(',', array_merge($blueprint->primary, array_column($blueprint->columns, 'name')));
        }
        $sql = "SELECT {$columns} FROM {$table}";

        if($blueprint->globalWhere) {
            $sql .= " WHERE " . $blueprint->globalWhere;
        }

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
     * Build SQL set for key-value array.
     *
     * @param array $primaryKeyValue
     *
     * @return string
     */
    protected function buildSetForArray($columns, $rowNum, $row)
    {
        $set = [];
        foreach ($columns as $column) {

            if ($column['replaceByFields']) {
                $row[$column['name']] = call_user_func($column['replaceByFields'], $row, $this->generator);
            }

            if ($column['replace']) {
                $row[$column['name']] = $this->calculateNewValue($column['replace'], $rowNum);
            }

            if (empty($column['where'])) {
                $set[] = "{$column['name']}='{$row[$column['name']]}'";
            } else {
                $set[] = "{$column['name']}=(
                    CASE 
                      WHEN {$column['where']} THEN '{$row[$column['name']]}'
                      ELSE {$column['name']}
                    END)";
            }
        }

        return implode(' ,', $set);
    }
}
