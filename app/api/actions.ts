import sql from '@/lib/db';

// Función para crear las tablas si no existen
export async function createTables() {
  // Crear tabla casino
  await sql`
    CREATE TABLE IF NOT EXISTS casino (
      id SERIAL PRIMARY KEY,
      table_id TEXT UNIQUE NOT NULL,
      table_name TEXT NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `;

  // Crear tabla roulette_events con foreign key a casino
  await sql`
    CREATE TABLE IF NOT EXISTS roulette_events (
      id SERIAL PRIMARY KEY,
      event_id TEXT UNIQUE NOT NULL,
      started_at TIMESTAMP WITH TIME ZONE NOT NULL,
      settled_at TIMESTAMP WITH TIME ZONE NOT NULL,
      outcome_number INTEGER NOT NULL,
      outcome_type TEXT NOT NULL,
      outcome_color TEXT NOT NULL,
      casino_id INTEGER REFERENCES casino(id),
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `;

  // Crear tabla para logging de errores
  await sql`
    CREATE TABLE IF NOT EXISTS error_logs (
      id SERIAL PRIMARY KEY,
      error_message TEXT NOT NULL,
      error_stack TEXT,
      context TEXT,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
  `;
}

// Función para registrar errores con filtro de 24 horas
export async function logError(error: Error, context?: string) {
  try {
    const errorMessage = error.message;
    const errorStack = error.stack;

    // Verificar si ya existe un error similar en las últimas 24 horas
    const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const recentErrorResult = await sql`
      SELECT id FROM error_logs
      WHERE created_at > ${twentyFourHoursAgo}
      LIMIT 1
    `;

    // Si ya existe un error similar en las últimas 24 horas, no registrar
    if (recentErrorResult.length > 0) {
      return;
    }

    // Registrar el nuevo error
    await sql`
      INSERT INTO error_logs (error_message, error_stack, context)
      VALUES (${errorMessage}, ${errorStack ||''}, ${context || 'processRouletteData'})
    `.catch(() => {
      // Si falla la inserción, al menos loguear en consola
      console.error('Error al insertar en error_logs:', error);
    });

  } catch (logError) {
    // Si falla el logging, al menos loguear en consola
    console.error('Error al registrar error en base de datos:', logError);
  }
}