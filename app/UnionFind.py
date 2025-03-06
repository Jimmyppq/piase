class UnionFind:
    def __init__(self):
        self.parent = {}  # Padre de cada nodo
        self.size = {}    # Tamaño del conjunto (1 o 2)

    def find(self, transaction_id):
        # Path compression (aunque la profundidad máxima es 1)
        if transaction_id not in self.parent:
            return None
        
        if self.parent[transaction_id] != transaction_id:
            self.parent[transaction_id] = self.find(self.parent[transaction_id])
        return self.parent[transaction_id]

    def union(self, trx1, trx2):
        """Une dos transacciones, asumiendo que ambas son raíces (size=1)."""
        root1 = self.find(trx1)
        root2 = self.find(trx2)

        if root1 != root2:
            # Enlazar root2 bajo root1 y actualizar tamaño
            self.parent[root2] = root1
            self.size[root1] = 2  # ¡Siempre será 2!

    def add_transaction(self, transaction_id):
        """Añade una transacción como conjunto unitario."""
        if transaction_id not in self.parent:
            self.parent[transaction_id] = transaction_id
            self.size[transaction_id] = 1  # Tamaño inicial

    def get_tree_size(self, transaction_id):
        """Devuelve 1 o 2 en tiempo constante."""
        root = self.find(transaction_id)
        if root is None:
            return 0
        return self.size[root]