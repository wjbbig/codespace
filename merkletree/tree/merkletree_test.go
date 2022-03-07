package tree

import "testing"

func TestMerkleTree_Build(t *testing.T) {
	tree := &MerkleTree{}
	tree.AppendLeaf([]byte("leaf1"))
	tree.AppendLeaf([]byte("leaf3"))
	tree.AppendLeaf([]byte("leaf2"))
	//tree.AppendLeaf([]byte("leaf4"))

	err := tree.Build()
	if err != nil {
		t.Fatal(err)
	}
}
