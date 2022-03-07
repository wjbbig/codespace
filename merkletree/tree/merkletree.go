package tree

import "crypto/sha256"

type Node struct {
	leftSubNode  *Node
	rightSubNode *Node
	leaf         bool
	dup          bool
	Data         []byte
	NodeHash     []byte
}

func (n *Node) CalHash() {
	hasher := sha256.New()
	hasher.Write(n.Data)
	nodeHash := hasher.Sum(nil)
	n.NodeHash = nodeHash[:]
}

type MerkleTree struct {
	Root     *Node
	RootHash []byte
	Leafs    []*Node
}

type MerkleProof struct {
	MerkleRoot     *Node
	TargetNodeHash []byte
	Bitmap         []int
	HashList       [][]byte
}

func (proof *MerkleProof) VerifyProof() error {
	return nil
}

func (tree *MerkleTree) Build() error {
	if len(tree.Leafs)%2 != 0 {
		dupLeaf := &Node{
			leaf:     true,
			dup:      true,
			Data:     tree.Leafs[len(tree.Leafs)-1].Data,
			NodeHash: tree.Leafs[len(tree.Leafs)-1].NodeHash,
		}
		tree.Leafs = append(tree.Leafs, dupLeaf)
	}
	root, err := buildRootNode(tree.Leafs)
	if err != nil {
		return err
	}

	tree.Root = root
	tree.RootHash = root.NodeHash
	return nil
}

func buildRootNode(nodes []*Node) (*Node, error) {
	var middleNodes []*Node
	if len(nodes) == 2 {
		root := buildMiddleNode(nodes)
		return root, nil
	}

	for i := 0; i < len(nodes); i += 2 {
		tmpNodes := make([]*Node, 2, 2)
		tmpNodes[0] = nodes[i]
		tmpNodes[1] = nodes[i+1]
		middleNodes = append(middleNodes, buildMiddleNode(tmpNodes))
	}

	return buildRootNode(middleNodes)
}

// buildMiddleNode 生成中间节点
func buildMiddleNode(nodes []*Node) *Node {
	hasher := sha256.New()
	hasher.Write(nodes[0].NodeHash)
	hasher.Write(nodes[1].NodeHash)
	hash := hasher.Sum(nil)
	return &Node{
		leftSubNode:  nodes[0],
		rightSubNode: nodes[1],
		NodeHash:     hash[:],
	}
}

func (tree *MerkleTree) AppendLeaf(data []byte) {
	leaf := &Node{
		leaf: true,
		Data: data,
	}
	leaf.CalHash()
	tree.Leafs = append(tree.Leafs, leaf)
}

func (tree *MerkleTree) GenerateMerkleProof(leftHash []byte) (*MerkleProof, error) {
	return nil, nil
}
