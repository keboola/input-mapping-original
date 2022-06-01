<?php

declare(strict_types=1);

namespace Keboola\InputMapping\Tests\Functional\Configuration\File\Manifest;

use Keboola\InputMapping\Configuration\Adapter as BaseAdapter;
use Keboola\InputMapping\Configuration\File\Manifest\Adapter;
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

        $adapter->setConfig(['id' => 12345678]);
        $adapter->writeToFile($filePathname);

        if ($adapter->getFormat() === BaseAdapter::FORMAT_JSON) {
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
            'id' => 12345678,
            'is_public' => false,
            'is_encrypted' => false,
            'is_sliced' => false,
            'tags' => [],
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
