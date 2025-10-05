# ============================================================================
# GENERADOR DE DATASETS PARA ACTIVIDAD PANDAS VS SPARK
# Ejecutar este script ANTES de la actividad para crear los CSVs
# ============================================================================

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

print("=" * 80)
print("GENERANDO DATASETS PARA ACTIVIDAD: E-COMMERCE GLOBAL")
print("=" * 80)

# Configuración de semilla para reproducibilidad
np.random.seed(42)
random.seed(42)

# ============================================================================
# DATASET 1: PRODUCTS (50,000 productos)
# ============================================================================

print("\n📦 Generando products.csv...")

# Categorías de productos
categories = [
    'Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors',
    'Books', 'Toys & Games', 'Beauty & Personal Care', 'Food & Beverages',
    'Automotive', 'Health & Wellness'
]

# Subcategorías por categoría
subcategories = {
    'Electronics': ['Smartphones', 'Laptops', 'Headphones', 'Cameras', 'Smartwatches'],
    'Clothing': ['Shirts', 'Pants', 'Dresses', 'Shoes', 'Accessories'],
    'Home & Garden': ['Furniture', 'Kitchen', 'Bedding', 'Decor', 'Tools'],
    'Sports & Outdoors': ['Fitness', 'Camping', 'Cycling', 'Swimming', 'Team Sports'],
    'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Comics', 'Magazines'],
    'Toys & Games': ['Action Figures', 'Board Games', 'Puzzles', 'Educational', 'Video Games'],
    'Beauty & Personal Care': ['Skincare', 'Makeup', 'Haircare', 'Fragrances', 'Bath'],
    'Food & Beverages': ['Snacks', 'Beverages', 'Canned', 'Frozen', 'Fresh'],
    'Automotive': ['Parts', 'Accessories', 'Tools', 'Care', 'Electronics'],
    'Health & Wellness': ['Vitamins', 'Supplements', 'Medical', 'Fitness', 'Personal Care']
}

num_products = 50000

# Generar productos
products_data = []
for i in range(num_products):
    product_id = i + 1
    category = random.choice(categories)
    subcategory = random.choice(subcategories[category])
    product_name = f"{subcategory} Item {product_id}"
    
    # Precio base según categoría
    if category == 'Electronics':
        base_price = np.random.uniform(50, 2000)
    elif category in ['Clothing', 'Beauty & Personal Care']:
        base_price = np.random.uniform(10, 200)
    elif category == 'Home & Garden':
        base_price = np.random.uniform(20, 500)
    else:
        base_price = np.random.uniform(5, 150)
    
    price = round(base_price, 2)
    cost = round(price * np.random.uniform(0.4, 0.7), 2)
    stock = np.random.randint(0, 1000)
    rating = round(np.random.uniform(1.5, 5.0), 1)
    num_reviews = np.random.randint(0, 5000)
    
    products_data.append({
        'product_id': product_id,
        'product_name': product_name,
        'category': category,
        'subcategory': subcategory,
        'price': price,
        'cost': cost,
        'stock_quantity': stock,
        'rating': rating,
        'num_reviews': num_reviews,
        'brand': f"Brand_{np.random.randint(1, 200)}"
    })

df_products = pd.DataFrame(products_data)
df_products.to_csv('products.csv', index=False)
print(f"✓ products.csv creado: {len(df_products):,} productos")
print(f"  Tamaño: {df_products.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

# ============================================================================
# DATASET 2: USER_ACTIVITY (5 millones de eventos)
# ============================================================================

print("\n👤 Generando user_activity.csv...")

num_users = 100000  # 100K usuarios
num_activities = 5000000  # 5M eventos de actividad

# Tipos de actividad
activity_types = ['view', 'add_to_cart', 'remove_from_cart', 'wishlist', 'search', 'review']
devices = ['mobile', 'desktop', 'tablet']
traffic_sources = ['organic', 'paid_search', 'social', 'direct', 'email', 'referral']

# Fecha de inicio y fin
end_date = datetime.now()
start_date = end_date - timedelta(days=365)

print(f"  Generando {num_activities:,} eventos de actividad...")

# Generar actividades en chunks para eficiencia
chunk_size = 500000
activities_chunks = []

for chunk_idx in range(num_activities // chunk_size):
    chunk_activities = []
    
    for _ in range(chunk_size):
        activity_id = chunk_idx * chunk_size + len(chunk_activities) + 1
        user_id = np.random.randint(1, num_users + 1)
        product_id = np.random.randint(1, num_products + 1)
        activity_type = random.choice(activity_types)
        
        # Timestamp aleatorio en el último año
        random_seconds = np.random.randint(0, int((end_date - start_date).total_seconds()))
        timestamp = start_date + timedelta(seconds=random_seconds)
        
        device = random.choice(devices)
        traffic_source = random.choice(traffic_sources)
        session_id = f"session_{user_id}_{np.random.randint(1, 50)}"
        
        # Duración de la actividad (en segundos)
        duration = np.random.randint(5, 600) if activity_type == 'view' else np.random.randint(1, 60)
        
        chunk_activities.append({
            'activity_id': activity_id,
            'user_id': user_id,
            'product_id': product_id,
            'activity_type': activity_type,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'device': device,
            'traffic_source': traffic_source,
            'session_id': session_id,
            'duration_seconds': duration
        })
    
    activities_chunks.append(pd.DataFrame(chunk_activities))
    print(f"  Chunk {chunk_idx + 1}/{num_activities // chunk_size} completado")

df_activity = pd.concat(activities_chunks, ignore_index=True)

# Ordenar por timestamp
df_activity = df_activity.sort_values('timestamp').reset_index(drop=True)

df_activity.to_csv('user_activity.csv', index=False)
print(f"✓ user_activity.csv creado: {len(df_activity):,} eventos")
print(f"  Tamaño: {df_activity.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

# ============================================================================
# DATASET 3: ORDERS (2 millones de órdenes)
# ============================================================================

print("\n🛒 Generando orders.csv...")

num_orders = 2000000  # 2M órdenes

# Estados de órdenes
order_statuses = ['completed', 'processing', 'shipped', 'cancelled', 'returned']
payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer', 'cash_on_delivery']

print(f"  Generando {num_orders:,} órdenes...")

# Generar órdenes en chunks
chunk_size = 200000
orders_chunks = []

for chunk_idx in range(num_orders // chunk_size):
    chunk_orders = []
    
    for _ in range(chunk_size):
        order_id = chunk_idx * chunk_size + len(chunk_orders) + 1
        user_id = np.random.randint(1, num_users + 1)
        product_id = np.random.randint(1, num_products + 1)
        
        # Buscar el precio del producto
        product_price = df_products.loc[df_products['product_id'] == product_id, 'price'].values[0]
        
        quantity = np.random.randint(1, 5)
        subtotal = round(product_price * quantity, 2)
        
        # Descuento aleatorio (0-30%)
        discount_percent = np.random.choice([0, 5, 10, 15, 20, 30], p=[0.5, 0.2, 0.15, 0.1, 0.04, 0.01])
        discount = round(subtotal * discount_percent / 100, 2)
        
        # Impuestos (16%)
        tax = round((subtotal - discount) * 0.16, 2)
        
        # Envío
        shipping = round(np.random.uniform(0, 20), 2) if np.random.random() > 0.3 else 0
        
        total_amount = round(subtotal - discount + tax + shipping, 2)
        
        # Fecha de orden (debe ser después de alguna actividad)
        random_seconds = np.random.randint(0, int((end_date - start_date).total_seconds()))
        order_date = start_date + timedelta(seconds=random_seconds)
        
        # Fecha de entrega (3-15 días después)
        delivery_days = np.random.randint(3, 16)
        delivery_date = order_date + timedelta(days=delivery_days)
        
        status = random.choice(order_statuses)
        payment_method = random.choice(payment_methods)
        
        chunk_orders.append({
            'order_id': order_id,
            'user_id': user_id,
            'product_id': product_id,
            'quantity': quantity,
            'subtotal': subtotal,
            'discount': discount,
            'tax': tax,
            'shipping': shipping,
            'total_amount': total_amount,
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'delivery_date': delivery_date.strftime('%Y-%m-%d %H:%M:%S'),
            'status': status,
            'payment_method': payment_method
        })
    
    orders_chunks.append(pd.DataFrame(chunk_orders))
    print(f"  Chunk {chunk_idx + 1}/{num_orders // chunk_size} completado")

df_orders = pd.concat(orders_chunks, ignore_index=True)

# Ordenar por fecha
df_orders = df_orders.sort_values('order_date').reset_index(drop=True)

df_orders.to_csv('orders.csv', index=False)
print(f"✓ orders.csv creado: {len(df_orders):,} órdenes")
print(f"  Tamaño: {df_orders.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

# ============================================================================
# RESUMEN Y ESTADÍSTICAS
# ============================================================================

print("\n" + "=" * 80)
print("📊 RESUMEN DE DATASETS GENERADOS")
print("=" * 80)

total_size = (
    df_products.memory_usage(deep=True).sum() +
    df_activity.memory_usage(deep=True).sum() +
    df_orders.memory_usage(deep=True).sum()
) / 1024 / 1024

print(f"""
┌─────────────────────────────────────────────────────────────────┐
│                    DATASETS CREADOS                             │
├─────────────────────────────────────────────────────────────────┤
│ 1. products.csv                                                 │
│    - Registros: {len(df_products):,}                                        │
│    - Columnas: {len(df_products.columns)}                                                  │
│    - Tamaño: {df_products.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB                                         │
│                                                                 │
│ 2. user_activity.csv                                            │
│    - Registros: {len(df_activity):,}                                  │
│    - Columnas: {len(df_activity.columns)}                                                  │
│    - Tamaño: {df_activity.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB                                        │
│    - Periodo: {df_activity['timestamp'].min()} a {df_activity['timestamp'].max()} │
│                                                                 │
│ 3. orders.csv                                                   │
│    - Registros: {len(df_orders):,}                                    │
│    - Columnas: {len(df_orders.columns)}                                                 │
│    - Tamaño: {df_orders.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB                                        │
│    - Periodo: {df_orders['order_date'].min()} a {df_orders['order_date'].max()} │
│                                                                 │
│ TAMAÑO TOTAL: {total_size:.2f} MB                                         │
└─────────────────────────────────────────────────────────────────┘
""")

print("\n📈 ESTADÍSTICAS RÁPIDAS:")
print(f"""
Productos:
  - Categorías: {df_products['category'].nunique()}
  - Precio promedio: ${df_products['price'].mean():.2f}
  - Producto más caro: ${df_products['price'].max():.2f}
  - Producto más barato: ${df_products['price'].min():.2f}

Actividad de Usuarios:
  - Usuarios únicos: {df_activity['user_id'].nunique():,}
  - Productos vistos: {df_activity[df_activity['activity_type']=='view'].shape[0]:,}
  - Items añadidos al carrito: {df_activity[df_activity['activity_type']=='add_to_cart'].shape[0]:,}
  - Dispositivo más usado: {df_activity['device'].mode()[0]}

Órdenes:
  - Usuarios que compraron: {df_orders['user_id'].nunique():,}
  - Tasa de conversión: {(df_orders['user_id'].nunique() / df_activity['user_id'].nunique() * 100):.2f}%
  - Ticket promedio: ${df_orders['total_amount'].mean():.2f}
  - Ventas totales: ${df_orders['total_amount'].sum():,.2f}
  - Estado más común: {df_orders['status'].mode()[0]}
""")

print("\n" + "=" * 80)
print("✅ TODOS LOS DATASETS HAN SIDO GENERADOS EXITOSAMENTE")
print("=" * 80)
print("\nArchivos creados en el directorio actual:")
print("  📄 products.csv")
print("  📄 user_activity.csv")
print("  📄 orders.csv")
print("\n💡 Estos archivos están listos para usar en la actividad Pandas vs Spark")
print("🚀 Los alumnos pueden cargarlos directamente con pd.read_csv() o spark.read.csv()")
print("\n" + "=" * 80)