#!/usr/bin/env node

/**
 * Script para procesar datos de roulette desde la API de casinoscores
 *
 * Uso:
 * node scripts/process-roulette-data.js
 */

const https = require('https');
const { z } = require('zod');

// Configuración desde variables de entorno
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error('❌ Error: DATABASE_URL no está configurada');
  process.exit(1);
}

// Esquemas de validación con Zod
const RouletteOutcomeSchema = z.object({
  number: z.number(),
  type: z.string(),
  color: z.string()
});

const RouletteTableSchema = z.object({
  id: z.string(),
  name: z.string()
});

const RouletteDataSchema = z.object({
  id: z.string(),
  startedAt: z.string(),
  settledAt: z.string(),
  status: z.string(),
  gameType: z.string(),
  table: RouletteTableSchema,
  result: z.object({
    outcome: RouletteOutcomeSchema,
    luckyNumbersList: z.array(z.object({
      number: z.number(),
      roundedMultiplier: z.number()
    })).optional()
  })
});

const RouletteEventSchema = z.object({
  id: z.string(),
  data: RouletteDataSchema
});

const RouletteEventsArraySchema = z.array(RouletteEventSchema);

// Función auxiliar para hacer peticiones HTTP
function makeRequest(url, options = {}, data = null) {
  return new Promise((resolve, reject) => {
    const urlObj = new URL(url);
    const lib = https;

    const reqOptions = {
      hostname: urlObj.hostname,
      port: urlObj.port || 443,
      path: urlObj.pathname + urlObj.search,
      method: options.method || 'GET',
      headers: options.headers || {}
    };

    const req = lib.request(reqOptions, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          const jsonData = JSON.parse(body);
          resolve({ status: res.statusCode, data: jsonData });
        } catch (e) {
          resolve({ status: res.statusCode, data: body });
        }
      });
    });

    req.on('error', reject);

    if (data) {
      req.write(typeof data === 'string' ? data : JSON.stringify(data));
    }

    req.end();
  });
}

// Función para conectar a PostgreSQL usando postgres
function createPostgresConnection() {
  const { default: postgres } = require('postgres');
  return postgres(DATABASE_URL, { ssl: 'require' });
}

// Función para registrar errores con filtro de 24 horas
async function logError(sql, error, context) {
  try {
    const errorMessage = error.message;
    const errorStack = error.stack;

    // Verificar si ya existe un error similar en las últimas 24 horas
    const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

    const recentErrorResult = await sql`
      SELECT id FROM error_logs
      WHERE error_message = ${errorMessage}
      AND created_at > ${twentyFourHoursAgo}
      LIMIT 1
    `;

    // Si ya existe un error similar en las últimas 24 horas, no registrar
    if (recentErrorResult.length > 0) {
      return;
    }

    // Registrar el nuevo error
    await sql`
      INSERT INTO error_logs (error_message, error_stack, context)
      VALUES (${errorMessage}, ${errorStack}, ${context || 'processRouletteData'})
    `.catch(() => {
      // Si falla la inserción, al menos loguear en consola
      console.error('Error al insertar en error_logs:', error);
    });

  } catch (logError) {
    // Si falla el logging, al menos loguear en consola
    console.error('Error al registrar error en base de datos:', logError);
  }
}

// Función para obtener datos de la API de roulette
async function fetchRouletteData() {
  console.log('📡 Obteniendo datos de la API de roulette...');

  const response = await makeRequest('https://api.casinoscores.com/svc-evolution-game-events/api/megaroulette');

  if (response.status !== 200) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  // Validar datos con Zod
  const validatedData = RouletteEventsArraySchema.parse(response.data);
  console.log(`✅ Datos validados: ${validatedData.length} eventos`);

  return validatedData;
}

// Función principal para procesar datos
async function processRouletteData() {
  const sql = createPostgresConnection();

  try {
    console.log('🚀 Iniciando procesamiento de datos de roulette...');

    // Crear tablas si no existen
    await sql`
      CREATE TABLE IF NOT EXISTS casino (
        id SERIAL PRIMARY KEY,
        table_id TEXT UNIQUE NOT NULL,
        table_name TEXT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `;

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

    await sql`
      CREATE TABLE IF NOT EXISTS error_logs (
        id SERIAL PRIMARY KEY,
        error_message TEXT NOT NULL,
        error_stack TEXT,
        context TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
      )
    `;

    // Obtener datos de la API
    const events = await fetchRouletteData();

    // Filtrar eventos con status "Resolved"
    const resolvedEvents = events.filter(event => event.data.status === 'Resolved');

    if (resolvedEvents.length === 0) {
      console.log('ℹ️ No hay eventos "Resolved" para procesar');
      return { processed: 0 };
    }

    console.log(`📊 Procesando ${resolvedEvents.length} eventos "Resolved"`);

    // Crear mapa de latest_settled_at por casino
    const casinoLatestSettledMap = new Map();

    // Obtener mesas únicas para consultar latest_settled_at por casino
    const uniqueTableIds = new Set();
    resolvedEvents.forEach(event => {
      uniqueTableIds.add(event.data.table.id);
    });

    // Consultar latest_settled_at para cada casino en paralelo
    const casinoQueries = Array.from(uniqueTableIds).map(async (tableId) => {
      const result = await sql`
        SELECT MAX(re.settled_at) as latest_settled_at
        FROM roulette_events re
        JOIN casino c ON re.casino_id = c.id
        WHERE c.table_id = ${tableId}
      `;
      return { tableId, latestSettledAt: result[0]?.latest_settled_at };
    });

    const latestSettledResults = await Promise.all(casinoQueries);
    latestSettledResults.forEach(({ tableId, latestSettledAt }) => {
      if (latestSettledAt) {
        casinoLatestSettledMap.set(tableId, new Date(latestSettledAt));
      }
    });

    // Filtrar eventos con status "Resolved" y settled_at más reciente que el último registro por casino
    const filteredEvents = resolvedEvents.filter(event => {
      const tableId = event.data.table.id;
      const latestSettledAt = casinoLatestSettledMap.get(tableId);

      if (!latestSettledAt) return true; // Si no hay registros previos para este casino, incluir

      const eventSettledAt = new Date(event.data.settledAt);
      return eventSettledAt > latestSettledAt;
    });

    if (filteredEvents.length === 0) {
      console.log('ℹ️ No hay eventos nuevos para procesar');
      return { processed: 0 };
    }

    console.log(`🎯 ${filteredEvents.length} eventos nuevos para insertar`);

    // Cache para almacenar casino_id por table_id
    const casinoCache = new Map();

    // Procesar casinos en paralelo
    const uniqueTables = new Map();
    filteredEvents.forEach(event => {
      const { id, name } = event.data.table;
      if (!uniqueTables.has(id)) {
        uniqueTables.set(id, name);
      }
    });

    // Obtener o crear casinos en paralelo
    const casinoPromises = Array.from(uniqueTables).map(async ([tableId, tableName]) => {
      // Buscar en la base de datos
      const casinoResult = await sql`
        SELECT id FROM casino WHERE table_id = ${tableId} AND table_name = ${tableName};
      `;

      if (casinoResult.length === 0) {
        // Insertar nuevo casino
        const insertResult = await sql`
          INSERT INTO casino (table_id, table_name)
          VALUES (${tableId}, ${tableName})
          RETURNING id
        `;
        return { tableId, casinoId: insertResult[0].id };
      } else {
        return { tableId, casinoId: casinoResult[0].id };
      }
    });

    const casinoResults = await Promise.all(casinoPromises);
    casinoResults.forEach(({ tableId, casinoId }) => {
      casinoCache.set(tableId, casinoId);
    });

    // Preparar datos de eventos para inserción masiva
    const eventData = filteredEvents.map(event => ({
      event_id: event.data.id,
      started_at: event.data.startedAt,
      settled_at: event.data.settledAt,
      outcome_number: event.data.result.outcome.number,
      outcome_type: event.data.result.outcome.type,
      outcome_color: event.data.result.outcome.color,
      casino_id: casinoCache.get(event.data.table.id)
    }));

    // Inserción masiva de eventos de roulette
    await sql`
      INSERT INTO roulette_events ${sql(eventData, 'event_id', 'started_at', 'settled_at', 'outcome_number', 'outcome_type', 'outcome_color', 'casino_id')}
      ON CONFLICT (event_id) DO NOTHING
    `;

    console.log(`✅ Procesamiento completado: ${filteredEvents.length} eventos insertados`);
    return { processed: filteredEvents.length };

  } catch (error) {
    console.error('❌ Error procesando datos de roulette:', error.message);

    // Registrar el error en la base de datos
    await logError(sql, error, 'processRouletteData');

    throw error;
  } finally {
    await sql.end();
  }
}

// Función principal
async function main() {
  try {
    const result = await processRouletteData();
    console.log('🎉 ¡Procesamiento completado exitosamente!');
    console.log(JSON.stringify(result, null, 2));
    process.exit(0);
  } catch (error) {
    console.error('💥 Error fatal:', error.message);
    process.exit(1);
  }
}

// Ejecutar el script
if (require.main === module) {
  main();
}