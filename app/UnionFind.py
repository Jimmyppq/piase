class UnionFind:
    def __init__(self):
        self.parent = {}  # Guarda el padre de cada nodo
        self.rank = {}    # Guarda el "peso" de cada nodo para optimizar unión
        self.size = {} 

    def find(self, transaction_id):
        """
        Encuentra la raíz y retorna una tupla con (raíz, tamaño_conjunto)
        Returns:
            tuple: (None, 0) si la transacción no existe
            tuple: (root_id, size) si la transacción existe
        """
        if transaction_id not in self.parent:
            return None, 0  # Retornamos una tupla con None y tamaño 0
        
        # Compresión de camino y conteo de nodos
        if self.parent[transaction_id] != transaction_id:
            root, size = self.find(self.parent[transaction_id])
            self.parent[transaction_id] = root
            return root, self.size[root]
        
        return transaction_id, self.size[transaction_id]

    def union(self, trx1, trx2):
        """ Une dos transacciones, optimizando la estructura """
        result1 = self.find(trx1)
        result2 = self.find(trx2)
        
        if not result1 or not result2:
            return
            
        root1, size1 = result1
        root2, size2 = result2

        if root1 != root2:
            # Unir el árbol más pequeño al más grande
            if size1 < size2:
                self.parent[root1] = root2
                self.size[root2] = size1 + size2  # Actualizar tamaño
            else:
                self.parent[root2] = root1
                self.size[root1] = size1 + size2  # Actualizar tamaño

    def add_transaction(self, transaction_id):
        """ Añade una nueva transacción al conjunto """
        if transaction_id not in self.parent:
            self.parent[transaction_id] = transaction_id
            self.rank[transaction_id] = 0
            self.size[transaction_id] = 1  # Inicializar el tamaño en 1

