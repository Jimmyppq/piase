class UnionFind:
    def __init__(self):
        self.parent = {}  # Guarda el padre de cada nodo
        self.rank = {}    # Guarda el "peso" de cada nodo para optimizar unión

    def find(self, transaction_id):
        if self.parent[transaction_id] != transaction_id:
            self.parent[transaction_id] = self.find(self.parent[transaction_id])
        return self.parent[transaction_id]

    def union(self, trx1, trx2):
        """ Une dos transacciones, optimizando la estructura """
        root1 = self.find(trx1)
        root2 = self.find(trx2)

        if root1 != root2:
            if self.rank[root1] > self.rank[root2]:
                self.parent[root2] = root1
            elif self.rank[root1] < self.rank[root2]:
                self.parent[root1] = root2
            else:
                self.parent[root2] = root1
                self.rank[root1] += 1

    def add_transaction(self, transaction_id):
        """ Añade una nueva transacción al conjunto """
        if transaction_id not in self.parent:
            self.parent[transaction_id] = transaction_id
            self.rank[transaction_id] = 0