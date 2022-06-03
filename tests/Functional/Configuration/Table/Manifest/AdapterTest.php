<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional\Configuration\Table\Manifest;

use Keboola\InputMapping\Configuration\Adapter as BaseAdapter;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Exception\InputOperationException;
use Keboola\InputMapping\Tests\Functional\Configuration\AbstractManifestAdapterTest;
use Keboola\Temp\Temp;

class AdapterTest extends AbstractManifestAdapterTest
{
    /**
     * @dataProvider fileOperationsData
     */
    public function testFileOperations(
        ?string $format,
        string $expectedFilePathname
    ): void {
        $temp = new Temp('docker');
        $temp->initRunFolder();

        $filePathname = (string) $temp->createTmpFile();

        if ($format) {
            $adapter = new Adapter($format);
        } else {
            $adapter = new Adapter();
        }

        $adapter->setConfig(['id' => 'in.c-bucket.test']);
        $adapter->writeToFile($filePathname);

        if ($adapter->getFormat() === 'json') {
            self::assertStringEqualsFile($expectedFilePathname, file_get_contents($filePathname) . PHP_EOL);
            self::assertStringEqualsFile(
                $expectedFilePathname,
                $adapter->getContents($filePathname) . PHP_EOL
            );
        } else {
            self::assertFileEquals($expectedFilePathname, $filePathname);
            self::assertStringEqualsFile($expectedFilePathname, $adapter->getContents($filePathname));
        }

        self::assertSame([
            'id' => 'in.c-bucket.test',
            'primary_key' => [],
            'distribution_key' => [],
            'columns' => [],
            'metadata' => [],
            'column_metadata' => [],
        ], $adapter->readFromFile($filePathname));
    }

    public function testGetContentsOfNonExistingFileThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('File \'test\' not found.');

        $adapter->readFromFile('test');
    }

    public function testReadFromNonExistingFileThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InputOperationException::class);
        $this->expectExceptionMessage('File \'test\' not found.');

        $adapter->readFromFile('test');
    }
}
