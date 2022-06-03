<?php

namespace Keboola\InputMapping\Tests\Configuration\Table\Manifest;

use Generator;
use Keboola\InputMapping\Configuration\Table\Manifest\Adapter;
use Keboola\InputMapping\Tests\Configuration\AbstractManifestAdapterTest;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class AdapterTest extends AbstractManifestAdapterTest
{
    private function createAdapter(?string $format): Adapter
    {
        if ($format) {
            return new Adapter($format);
        }

        return new Adapter();
    }

    /**
     * @dataProvider initWithFormatData
     */
    public function testInitWithFormat(
        ?string $format,
        string $expectedFormat,
        string $expectedExtension
    ): void {
        $adapter = $this->createAdapter($format);

        self::assertSame($expectedFormat, $adapter->getFormat());
        self::assertSame($expectedExtension, $adapter->getFileExtension());
    }

    public function setConfigAndSerializeData(): Generator
    {
        yield 'default format' => [
            'format' => null,
            'expectedData' => <<<'EOF'
{
    "id": "in.c-bucket.test",
    "primary_key": [],
    "distribution_key": [],
    "columns": [],
    "metadata": [],
    "column_metadata": []
}
EOF,
        ];
        yield 'json format' => [
            'format' => 'json',
            'expectedData' => <<<'EOF'
{
    "id": "in.c-bucket.test",
    "primary_key": [],
    "distribution_key": [],
    "columns": [],
    "metadata": [],
    "column_metadata": []
}
EOF,
        ];
        yield 'yaml format' => [
            'format' => 'yaml',
            'expectedData' => <<<'EOF'
id: in.c-bucket.test
primary_key: {  }
distribution_key: {  }
columns: {  }
metadata: {  }
column_metadata: {  }

EOF,
        ];
    }

    /**
     * @dataProvider setConfigAndSerializeData
     */
    public function testSetConfigAndSerialize(
        ?string $format,
        string $expectedData
    ): void {
        $adapter = $this->createAdapter($format);
        $adapter->setConfig(['id' => 'in.c-bucket.test']);

        self::assertSame($expectedData, $adapter->serialize());
    }

    public function testSetInvalidConfigThrowsException(): void
    {
        $adapter = new Adapter();

        $this->expectException(InvalidConfigurationException::class);
        $this->expectExceptionMessage('The child config "id" under "table" must be configured.');

        $adapter->setConfig([]);
    }
}
