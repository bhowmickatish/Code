package cache

import "fmt"

// SlotKeys are Redis keys that share one cluster hash slot via a common {tag}.
type SlotKeys struct {
	Data   string
	Notify string
	Lock   string
}

func slotKeys(tag string) SlotKeys {
	braced := "{" + tag + "}"
	return SlotKeys{
		Data:   braced + ":data",
		Notify: braced + ":notify",
		Lock:   braced + ":lock",
	}
}

// ProductSlotKeys returns slot-aligned keys for a product cache entry.
func ProductSlotKeys(id int64) SlotKeys {
	return slotKeys(fmt.Sprintf("product:%d", id))
}

// IdempotencySlotKeys returns slot-aligned keys for an idempotency record.
func IdempotencySlotKeys(hashHex string) SlotKeys {
	return slotKeys("idempotency:" + hashHex)
}
