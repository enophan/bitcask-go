package data

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)
}

func TestWrite(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("file1"))
	assert.Nil(t, err)
}

func TestClose(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 2)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("feabhhabfhj"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)
}

func TestSync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 3)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("feabhhabfhj"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}
