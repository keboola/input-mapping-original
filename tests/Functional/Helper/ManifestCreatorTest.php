<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional\Helper;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\InputMapping\Configuration\Adapter as BaseAdapter;
use Keboola\InputMapping\Helper\ManifestCreator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\Metadata\TableMetadataUpdateOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class ManifestCreatorTest extends TestCase
{
    private Temp $temp;
    private ClientWrapper $clientWrapper;

    public function writeTableManifestData(): Generator
    {
        yield 'json format' => [
            'format' => 'json',
            'columns' => [],
            'expectedFilePathname' => __DIR__ . '/data/writeTableManifest-expected.json',
        ];
        yield 'yaml format with columns override' => [
            'format' => 'yaml',
            'columns' => ['Name'],
            'expectedFilePathname' => __DIR__ . '/data/writeTableManifest-columnsOverride-expected.yml',
        ];
        yield 'json format' => [
            'format' => 'json',
            'columns' => [],
            'expectedFilePathname' => __DIR__ . '/data/writeTableManifest-expected.json',
        ];
        yield 'json format with columns override' => [
            'format' => 'json',
            'columns' => ['Name'],
            'expectedFilePathname' => __DIR__ . '/data/writeTableManifest-columnsOverride-expected.json',
        ];
    }

    public function setUp()
    {
        parent::setUp();

        $this->clientWrapper = new ClientWrapper(
            new ClientOptions(STORAGE_API_URL, STORAGE_API_TOKEN),
        );

        $tokenInfo = $this->clientWrapper->getBasicClient()->verifyToken();
        print(sprintf(
            'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
            $tokenInfo['description'],
            $tokenInfo['id'],
            $tokenInfo['owner']['name'],
            $tokenInfo['owner']['id'],
            $this->clientWrapper->getBasicClient()->getApiUrl()
        ));

        // Create folders
        $temp = new Temp('docker');
        $temp->initRunFolder();
        $this->temp = $temp;

        try {
            $this->clientWrapper->getBasicClient()->dropBucket('in.c-docker-test', ['force' => true]);
        } catch (ClientException $e) {
            if ($e->getCode() != 404) {
                throw $e;
            }
        }
        $this->clientWrapper->getBasicClient()->createBucket('docker-test', Client::STAGE_IN, 'Docker Testsuite');

        // Create table
        $csv = new CsvFile($this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'upload.csv');
        $csv->writeRow(['Id', 'Name', 'foo', 'bar']);
        $csv->writeRow(['id1', 'name1', 'foo1', 'bar1']);
        $csv->writeRow(['id2', 'name2', 'foo2', 'bar2']);
        $csv->writeRow(['id3', 'name3', 'foo3', 'bar3']);
        $this->clientWrapper->getBasicClient()->createTableAsync('in.c-docker-test', 'test', $csv);

        (new Metadata($this->clientWrapper->getBasicClient()))->postTableMetadataWithColumns(new TableMetadataUpdateOptions(
            'in.c-docker-test.test',
            'input-mapping',
            [
                [
                    'key' => 'description',
                    'value' => 'Test',
                ],
            ],
            [
                'Id' => [
                    [
                        'key' => 'datatype',
                        'value' => 'NUMBER',
                    ],
                ],
                'Name' => [
                    [
                        'key' => 'datatype',
                        'value' => 'TEXT',
                    ],
                ],
            ]
        ));
    }

    /**
     * @dataProvider writeTableManifestData
     */
    public function testWriteTableManifest(
        ?string $format,
        array $columns,
        string $expectedFilePathname
    ): void {
        $filePathname = (string) $this->temp->createTmpFile();

        $tableInfo = $this->clientWrapper->getBasicClient()->getTable('in.c-docker-test.test');

        // override dynamic dateTimes and numeric ids to static data
        $tableInfo['created'] = '2022-06-03T01:31:43+0200';
        $tableInfo['lastChangeDate'] = '2022-06-03T02:31:43+0200';
        $tableInfo['lastImportDate'] = '2022-06-03T03:31:43+0200';

        $tableInfo['metadata'][0]['id'] = '123';
        $tableInfo['metadata'][0]['timestamp'] = '2022-06-03T04:31:43+0200';

        $tableInfo['columnMetadata']['Id'][0]['id'] = '456';
        $tableInfo['columnMetadata']['Id'][0]['timestamp'] = '2022-06-03T05:31:43+0200';
        $tableInfo['columnMetadata']['Name'][0]['id'] = '789';
        $tableInfo['columnMetadata']['Name'][0]['timestamp'] = '2022-06-03T06:31:43+0200';

        $manifestCreator = new ManifestCreator($this->clientWrapper->getBasicClient());
        $manifestCreator->writeTableManifest($tableInfo, $filePathname, $columns, $format);

        if ($format === BaseAdapter::FORMAT_JSON) {
            self::assertStringEqualsFile($expectedFilePathname, file_get_contents($filePathname) . PHP_EOL);
        } else {
            self::assertFileEquals($expectedFilePathname, $filePathname);
        }
    }
}
