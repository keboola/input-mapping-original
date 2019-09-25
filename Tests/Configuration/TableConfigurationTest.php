<?php

namespace Keboola\InputMapping\Tests\Configuration;

use Keboola\InputMapping\Configuration\Table;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class TableConfigurationTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @return mixed[][]
     */
    public function provideValidConfigs()
    {
        return [
            'ComplexConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => "-1 days",
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'DaysNullConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "days" => null,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'DaysConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "days" => 1,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'ChangedSinceNullConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => null,
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'ChangedSinceConfiguration' => [
                [
                    "source" => "in.c-main.test",
                    "destination" => "test",
                    "changed_since" => "-1 days",
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
            'SearchSourceConfiguration' => [
                [
                    "search_source" => [
                        "search_by" => "table",
                        "key" => "bdm.scaffold.tag",
                        "value" => "test_table",
                    ],
                    "destination" => "test",
                    "changed_since" => "-1 days",
                    "columns" => ["Id", "Name"],
                    "where_column" => "status",
                    "where_values" => ["val1", "val2"],
                    "where_operator" => "ne",
                ],
            ],
        ];
    }

    public function provideValidConfigsChanged()
    {
        return [
            'BasicConfiguration' => [
                [
                    "source" => "in.c-main.test",
                ],
                [
                    "source" => "in.c-main.test",
                    "columns" => [],
                    "where_values" => [],
                    "where_operator" => "eq",
                ],
            ],

        ];
    }

    /**
     * @dataProvider provideValidConfigsChanged
     */
    public function testValidConfigDefinitionChanged(
        array $config,
        array $expected
    ) {
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expected, $processedConfiguration);
    }

    /**
     * @dataProvider provideValidConfigs
     */
    public function testValidConfigDefinition(
        array $config
    ) {
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($config, $processedConfiguration);
    }

    /**
     * @return mixed[][]
     */
    public function provideInvalidConfigs()
    {
        return [
            'InvalidWhereOperator' => [
                [
                    "source" => "in.c-main.test",
                    "where_operator" => 'abc',
                ],
                InvalidConfigurationException::class,
                'Invalid configuration for path "table.where_operator": Invalid operator in where_operator "abc"',
            ],
            'testEmptyConfiguration' => [
                [],
                InvalidConfigurationException::class,
                'Either "source" or "search_source" must be defined',
            ],
            'EmptySourceConfiguration' => [
                ["source" => ""],
                InvalidConfigurationException::class,
                'The path "table.source" cannot contain an empty value, but got "".',
            ],
            'InvalidSearchSourceSearchBy' => [
                [
                    "search_source" => [
                        "search_by" => "invalidSearchBy",
                        "key" => "bdm.scaffold.tag",
                        "value" => "test_table",
                    ],
                ],
                InvalidConfigurationException::class,
                'The value "invalidSearchBy" is not allowed for path "table.search_source.search_by". Permissible values: "table"',
            ],
            'InvalidSearchSourceEmptyKey' => [
                [
                    "search_source" => [
                        "key" => "",
                        "value" => "test_table",
                    ],
                ],
                InvalidConfigurationException::class,
                'The path "table.search_source.key" cannot contain an empty value, but got "".',
            ],
            'InvalidSearchSourceEmptyValue' => [
                [
                    "search_source" => [
                        "key" => "bdm.scaffold.tag",
                        "value" => "",
                    ],
                ],
                InvalidConfigurationException::class,
                'The path "table.search_source.value" cannot contain an empty value, but got "".',
            ],
        ];
    }

    /**
     * @dataProvider provideInvalidConfigs
     * @param array $config
     * @param string $exception
     * @param string $exceptionMessage
     */
    public function testInvalidConfigDefinition(
        array $config,
        $exception,
        $exceptionMessage
    ) {
        $this->expectException($exception);
        $this->expectExceptionMessage($exceptionMessage);
        (new Table())->parse(["config" => $config]);
    }

    public function testEmptyWhereOperator()
    {
        $config = [
            "source" => "in.c-main.test",
            "where_operator" => "",
        ];

        $expectedArray = $config;
        $expectedArray["where_operator"] = "eq";
        $expectedArray["columns"] = [];
        $expectedArray["where_values"] = [];
        $processedConfiguration = (new Table())->parse(["config" => $config]);
        self::assertEquals($expectedArray, $processedConfiguration);
    }
}
