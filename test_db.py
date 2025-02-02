import asyncpg
import asyncio

DATABASE_URL = "postgresql://postgres:WhLuYoqHrGkeQtleZAEodiMoEEQcdVvV@postgres.railway.internal:5432/railway"

async def test_connection():
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Conexión exitosa a PostgreSQL en Railway")
        await conn.close()
    except Exception as e:
        print(f"❌ Error de conexión: {e}")

# Ejecuta la prueba
asyncio.run(test_connection())
