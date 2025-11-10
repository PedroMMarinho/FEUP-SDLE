CREATE TABLE ShoppingList (
    uuid UUID PRIMARY KEY,
    name TEXT NOT NULL,
    crdt JSONB NOT NULL
);


CREATE INDEX idx_shopping_list_item_list_uuid ON ShoppingListItem (shopping_list_uuid);

CREATE TABLE ShoppingListItem (
    id SERIAL PRIMARY KEY,
    shopping_list_uuid UUID NOT NULL REFERENCES ShoppingList(uuid) ON DELETE CASCADE,
    name TEXT NOT NULL,
    quantityNeeded INTEGER NOT NULL,
    quantityAcquired INTEGER NOT NULL,
    position INTEGER NOT NULL,
    CHECK (quantityNeeded >= 0 AND quantityAcquired >= 0)
);
