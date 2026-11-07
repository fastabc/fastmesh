// Package apply is the node's transactional commit path: validate and prepare, record a durable pending → committed decision, then publish; after the decision, failures recover forward and are never NACKed. Its API is unstable during v0.
package apply
