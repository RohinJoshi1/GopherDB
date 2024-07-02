package main

import (
	"bytes"
	"encoding/binary"
)

type Item struct {
	key   []byte
	value []byte
}

type Node struct {
	*dal

	pageNum    pgnum
	items      []*Item
	childNodes []pgnum
}

func NewEmptyNode() *Node {
	return &Node{}
}

// NewNodeForSerialization creates a new node only with the properties that are relevant when saving to the disk
func NewNodeForSerialization(items []*Item, childNodes []pgnum) *Node {
	return &Node{
		items:      items,
		childNodes: childNodes,
	}
}

func newItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func isLast(index int, parentNode *Node) bool {
	return index == len(parentNode.items)
}

func isFirst(index int) bool {
	return index == 0
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) writeNode(node *Node) *Node {
	node, _ = n.dal.writeNode(node)
	return node
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.dal.getNode(pageNum)
}

// isOverPopulated checks if the node size is bigger than the size of a page.
func (n *Node) isOverPopulated() bool {
	return n.dal.isOverPopulated(n)
}

// canSpareAnElement checks if the node size is big enough to populate a page after giving away one item.
func (n *Node) canSpareAnElement() bool {
	splitIndex := n.dal.getSplitIndex(n)
	if splitIndex == -1 {
		return false
	}
	return true
}

// isUnderPopulated checks if the node size is smaller than the size of a page.
func (n *Node) isUnderPopulated() bool {
	return n.dal.isUnderPopulated(n)
}

func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1

	// Add page header: isLeaf, key-value pairs count, node num
	// isLeaf
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	// key-value pairs count
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	//slotted pages for storing data in the page. It means the actual keys and values (the cells) are appended
	// to right of the page whereas offsets have a fixed size and are appended from the left.

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]
		if !isLeaf {
			childNode := n.childNodes[i]

			// Write the child page as a fixed size of 8 bytes
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}

		klen := len(item.key)
		vlen := len(item.value)

		// write offset
		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftPos:], uint16(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.value)

		rightPos -= 1
		buf[rightPos] = byte(vlen)

		rightPos -= klen
		copy(buf[rightPos:], item.key)

		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !isLeaf {
		// Write the last child node
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		// Write the child page as a fixed size of 8 bytes
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

func (n *Node) deserialize(buf []byte) {
	leftPos := 0

	// Read header
	isLeaf := uint16(buf[0])

	itemsCount := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	// Read body
	for i := 0; i < itemsCount; i++ {
		if isLeaf == 0 { // False
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize

			n.childNodes = append(n.childNodes, pgnum(pageNum))
		}

		// Read offset
		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += vlen
		n.items = append(n.items, newItem(key, value))
	}

	if isLeaf == 0 { // False
		// Read the last child node
		pageNum := pgnum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}

// elementSize returns the size of a key-value-childNode triplet at a given index.
// If the node is a leaf, then the size of a key-value pair is returned.
// It's assumed i <= len(n.items)
func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].key)
	size += len(n.items[i].value)
	size += pageNumSize // 8 is the pgnum size
	return size
}

// nodeSize returns the node's size in bytes
func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize

	for i := range n.items {
		size += n.elementSize(i)
	}

	// Add last page
	size += pageNumSize // 8 is the pgnum size
	return size
}

// findKey searches for a key inside the tree. Once the key is found, the parent node and the correct index are returned
// so the key itself can be accessed in the following way parent[index]. A list of the node ancestors (not including the
// node itself) is also returned.
// If the key isn't found, we have 2 options. If exact is true, it means we expect findKey
// If exact is false, then findKey is used to locate where a new key should be
// inserted so the position is returned.
func (n *Node) findKey(key []byte, exact bool) (int, *Node, []int, error) {
	ancestorsIndexes := []int{0} // index of root
	index, node, err := findKeyHelper(n, key, exact, &ancestorsIndexes)
	if err != nil {
		return -1, nil, nil, err
	}
	return index, node, ancestorsIndexes, nil
}

func findKeyHelper(node *Node, key []byte, exact bool, ancestorsIndexes *[]int) (int, *Node, error) {
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}

	if node.isLeaf() {
		if exact {
			return -1, nil, nil
		}
		return index, node, nil
	}

	*ancestorsIndexes = append(*ancestorsIndexes, index)
	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key, exact, ancestorsIndexes)
}

// findKeyInNode iterates all the items and finds the key. If the key is found, then the item is returned. If the key
// isn't found then return the index where it should have been (the first index that key is greater than it's previous)
func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range n.items {
		res := bytes.Compare(existingItem.key, key)
		if res == 0 { // Keys match
			return true, i
		}

		// The key is bigger than the previous item, so it doesn't exist in the node, but may exist in child nodes.
		if res == 1 {
			return false, i
		}
	}

	// The key isn't bigger than any of the items which means it's in the last index.
	return false, len(n.items)
}

func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.items) == insertionIndex { // nil or empty slice or after last element
		n.items = append(n.items, item)
		return insertionIndex
	}

	n.items = append(n.items[:insertionIndex+1], n.items[insertionIndex:]...)
	n.items[insertionIndex] = item
	return insertionIndex
}

// split rebalances the tree after adding. After insertion the modified node has to be checked to make sure it
// didn't exceed the maximum number of elements. If it did, then it has to be split and rebalanced. 
func (n *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {
	// The first index where min amount of bytes to populate a page is achieved. Then add 1 so it will be split one
	// index after.
	splitIndex := nodeToSplit.dal.getSplitIndex(nodeToSplit)

	middleItem := nodeToSplit.items[splitIndex]
	var newNode *Node

	if nodeToSplit.isLeaf() {
		newNode = n.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], []pgnum{}))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
	} else {
		newNode = n.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], nodeToSplit.childNodes[splitIndex+1:]))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}
	n.addItem(middleItem, nodeToSplitIndex)
	if len(n.childNodes) == nodeToSplitIndex+1 { // If middle of list, then move items forward
		n.childNodes = append(n.childNodes, newNode.pageNum)
	} else {
		n.childNodes = append(n.childNodes[:nodeToSplitIndex+1], n.childNodes[nodeToSplitIndex:]...)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNum
	}

	n.writeNodes(n, nodeToSplit)
}
//Deletion 

//Delete from LeafNode 
//Simply delete from node 

func(n *Node) removeItemFromLeaf(index int){
	n.items = append(n.items[:index], n.items[index+1:]...)
	n.writeNodes(n)
}

//Delete internal Node: Find rightmost child from left subtree, delete that pair from leaf, and replace node to be deleted with the rightmost child of LST  

func (n *Node) removeItemFromInternal(index int)([]int, error){
	affectedNodes := make([]int,0)
	affectedNodes = append(affectedNodes, index)
	//Get left tree 
	lNode, err := n.getNode(n.childNodes[index])
	if err!= nil {
		return nil,err
	}
	//Go right 
	for !lNode.isLeaf(){
		rIndex := len(n.childNodes)-1 
		lNode,err = lNode.getNode(lNode.childNodes[rIndex])
		if err!=nil{
			return nil,err
		}
		affectedNodes = append(affectedNodes, rIndex)
	}
	n.items[index] = lNode.items[len(lNode.items)-1]
	lNode.items = lNode.items[:len(lNode.items)-1]
	n.writeNodes(n,lNode)
	return affectedNodes,nil
}


//Helper functions for rotations 
func rotateRight(leftNode *Node, rightNode *Node,parentNode *Node, rightNodeIndex int){
	leftNodeItem := leftNode.items[len(leftNode.items)-1]
	leftNode.items = leftNode.items[:len(leftNode.items)-1]
	pNodeIndex := rightNodeIndex-1 
	if isFirst(pNodeIndex){
		pNodeIndex = 0 
	}
	pNodeItem := parentNode.items[pNodeIndex]
	parentNode.items[pNodeIndex] = leftNodeItem 
	rightNode.items = append([]*Item{pNodeItem},rightNode.items...)
	//Transfer any children 
	if !leftNode.isLeaf(){
		child := leftNode.childNodes[len(leftNode.childNodes)-1]
		leftNode.childNodes = leftNode.childNodes[:len(leftNode.childNodes)-1]
		rightNode.childNodes = append([]pgnum{child},rightNode.childNodes...)
	}

}
func rotateLeft(leftNode *Node, rightNode *Node,parentNode *Node, rightNodeIndex int){
	rightNodeItem := rightNode.items[0]
	rightNode.items = rightNode.items[1:]
	pNodeIndex := rightNodeIndex
	if isLast(pNodeIndex,parentNode){
		pNodeIndex = len(parentNode.items)-1 
	}
	pNodeItem := parentNode.items[pNodeIndex]
	parentNode.items[pNodeIndex] = rightNodeItem 
	leftNode.items = append(leftNode.items,pNodeItem)
	//Transfer any children 
	if !rightNode.isLeaf(){
		child := rightNode.childNodes[0]
		rightNode.childNodes = rightNode.childNodes[1:]
		leftNode.childNodes = append(leftNode.childNodes,child)
	}
}
//Merge: receive node and index, transfer node to left child with it's KV pairs and child pointers and delete node 
//Needs to be accompanied by rebalance later 

func (n *Node) merge(bNode *Node, bNodeIndex int) error {
	// 	               p                                     p
	//                    3,5                                    5
	//	      /        |       \       ------>         /          \
	//          a          b        c                     a            c
	//         1,2         4        6,7                 1,2,3,4         6,7
	aNode, err := n.getNode(n.childNodes[bNodeIndex-1])
	if err!=nil{
		return err 
	}
	// Take the item from the parent, remove it and add it to the unbalanced node
	pNodeItem := n.items[bNodeIndex-1]
	n.items = append(n.items[:bNodeIndex-1], n.items[bNodeIndex:]...)
	aNode.items = append(aNode.items, pNodeItem)

	aNode.items = append(aNode.items, bNode.items...)
	n.childNodes = append(n.childNodes[:bNodeIndex], n.childNodes[bNodeIndex+1:]...)
	if !aNode.isLeaf() {
		aNode.childNodes = append(aNode.childNodes, bNode.childNodes...)
	}

	n.writeNodes(aNode, n)
	n.dal.deleteNode(bNode.pageNum)
	return nil
}
//3 Cases: Left rotate, right rotate , merge
func (n *Node) rebalanceRemove(unabalancedNode *Node, unbalancedNodeIndex int) error {
	parent := n
	//I can right rotate
	if unbalancedNodeIndex != 0{
		leftNode, err := n.getNode(parent.childNodes[unbalancedNodeIndex-1])
		if err!=nil{
			return err 
		}
		if leftNode.canSpareAnElement(){
			rotateRight(leftNode, unabalancedNode, parent, unbalancedNodeIndex)
			n.writeNodes(leftNode, parent,unabalancedNode)
			return nil
		}
	}
	if unbalancedNodeIndex != len(parent.childNodes)-1{
		rightNode,err := n.getNode(parent.childNodes[unbalancedNodeIndex+1])
		if err!=nil{
			return err 
		}
		if rightNode.canSpareAnElement(){
			rotateLeft(unabalancedNode,rightNode,parent,unbalancedNodeIndex)
			n.writeNodes(unabalancedNode, parent,rightNode)
			return nil
		}
	}
	if unbalancedNodeIndex == 0{
		rightNode, err := n.getNode(parent.childNodes[unbalancedNodeIndex+1])
		if err!=nil{
			return err 
		}
		return parent.merge(rightNode,unbalancedNodeIndex+1)
	}
	return parent.merge(unabalancedNode,unbalancedNodeIndex) 
}



