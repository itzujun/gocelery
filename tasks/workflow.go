package tasks

import (
	"fmt"
	"github.com/google/uuid"
)

type Chain struct {
	Tasks []*Signature
}

type Group struct {
	GroupUUid string
	Tasks     []*Signature
}

type Chord struct {
	Group    *Group
	Callback *Signature
}

func (group *Group) GetUUIDs() [] string {
	taskUUIDs := make([]string, len(group.Tasks))
	for i, signature := range group.Tasks {
		taskUUIDs[i] = signature.UUID
	}
	return taskUUIDs
}

func NewChain(signatures ...*Signature) (*Chain, error) {

	for _, signature := range signatures {
		if signature.UUID == "" {
			signatureID := uuid.New().String()
			signature.UUID = fmt.Sprintf("task_%v", signatureID)
		}
	}

	for i := len(signatures) - 1; i > 0; i-- {
		if i > 0 {
			signatures[i-1].OnSuccess = []*Signature{signatures[i]}
		}
	}
	chain := &Chain{Tasks: signatures}
	return chain, nil
}

func NewGroup(signatures ...*Signature) (*Group, error) {
	groupUUID := uuid.New().String()
	groupID := fmt.Sprintf("group_%v", groupUUID)
	for _, signature := range signatures {
		if signature.UUID == "" {
			signatureID := uuid.New().String()
			signature.UUID = fmt.Sprintf("task_%v", signatureID)
		}
		signature.GroupUUID = groupID
		signature.GroupTaskCount = len(signatures)
	}
	return &Group{
		GroupUUid: groupID,
		Tasks:     signatures,
	}, nil
}

func NewChord(group *Group, callback *Signature) (*Chord, error) {
	if callback.UUID == "" {
		callbackUUID := uuid.New().String()
		callback.UUID = fmt.Sprintf("chord_%v", callbackUUID)
	}

	for _, signature := range group.Tasks {
		signature.ChordCallback = callback
	}
	return &Chord{Group: group, Callback: callback}, nil

}
