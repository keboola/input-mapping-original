<?php

namespace Keboola\InputMapping\Tests\Reader;

use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InvalidInputException;
use Keboola\InputMapping\Reader\Options\InputTableOptionsList;
use Keboola\InputMapping\Reader\Reader;
use Keboola\InputMapping\Reader\State\InputTableStateList;
use Keboola\StorageApi\ClientException;
use Psr\Log\Test\TestLogger;

class DownloadTablesWorkspaceSnowflakeTest extends DownloadTablesWorkspaceTestAbstract
{
    public function testTablesSnowflakeBackend()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->client, $logger, $this->getWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
                'where_column' => 'Id',
                'where_values' => ['Id2', 'Id3'],
            ],
            [
                'source' => 'in.c-input-mapping-test.test3',
                'destination' => 'test3',
            ],
        ]);

        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'workspace-snowflake'
        );

        $adapter = new Adapter();

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test1.manifest');
        self::assertEquals('in.c-input-mapping-test.test1', $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->client->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test1', 'name' => 'test1']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('Invalid columns: _timestamp:', $e->getMessage());
        }

        // this is copy, so it doesn't contain the _timestamp column
        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test2.manifest');
        self::assertEquals('in.c-input-mapping-test.test2', $manifest['id']);
        $this->client->createTableAsyncDirect(
            'out.c-input-mapping-test',
            ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test2', 'name' => 'test2']
        );

        $manifest = $adapter->readFromFile($this->temp->getTmpFolder() . '/download/test3.manifest');
        self::assertEquals('in.c-input-mapping-test.test3', $manifest['id']);
        /* we want to check that the table exists in the workspace, so we try to load it, which fails, because of
            the _timestamp columns, but that's okay. It means that the table is indeed in the workspace. */
        try {
            $this->client->createTableAsyncDirect(
                'out.c-input-mapping-test',
                ['dataWorkspaceId' => $this->workspaceId, 'dataTableName' => 'test3', 'name' => 'test3']
            );
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertContains('Invalid columns: _timestamp:', $e->getMessage());
        }

        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test1" will be cloned.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test2" will be copied.'));
        self::assertTrue($logger->hasInfoThatContains('Table "in.c-input-mapping-test.test3" will be cloned.'));
        self::assertTrue($logger->hasInfoThatContains('Cloning 2 tables to snowflake workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Copying 1 tables to snowflake workspace.'));
        self::assertTrue($logger->hasInfoThatContains('Processing 2 workspace exports.'));
        // test that the clone jobs are merged into a single one
        $jobs = $this->client->listJobs(['limit' => 10]);
        $params = null;
        foreach ($jobs as $job) {
            if ($job['operationName'] === 'workspaceLoadClone') {
                $params = $job['operationParams'];
            }
        }
        self::assertNotEmpty($params);
        self::assertEquals(2, count($params['input']));
        self::assertEquals('test1', $params['input'][0]['destination']);
        self::assertEquals('test3', $params['input'][1]['destination']);
    }

    public function testTablesInvalidMapping()
    {
        $logger = new TestLogger();
        $reader = new Reader($this->client, $logger, $this->getWorkspaceProvider());
        $configuration = new InputTableOptionsList([
            [
                'source' => 'in.c-input-mapping-test.test1',
                'destination' => 'test1',
                'changed_since' => 'adaptive',
            ],
            [
                'source' => 'in.c-input-mapping-test.test2',
                'destination' => 'test2',
            ]
        ]);

        self::expectException(InvalidInputException::class);
        self::expectExceptionMessage('Adaptive input mapping is not supported on input mapping to workspace.');
        $reader->downloadTables(
            $configuration,
            new InputTableStateList([]),
            $this->temp->getTmpFolder() . DIRECTORY_SEPARATOR . 'download',
            'workspace-snowflake'
        );
    }
}