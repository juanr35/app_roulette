import sql from '@/lib/db';
import { createTables, logError } from '@/app/api/actions'; 
import { RouletteEventSchema } from '@/app/api/scores/schemas';

const RouletteEventsArraySchema = RouletteEventSchema.array();

// Función para obtener datos de la API de roulette
async function fetchRouletteData() {
  const response = await fetch('https://api.casinoscores.com/svc-evolution-game-events/api/megaroulette');

  if (!response.ok) {
    throw new Error(`HTTP error! status: ${response.status}`);
  }

  const data = await response.json();

  return data
}

// Función para procesar y almacenar los datos de roulette
async function processRouletteData() {
  try {
    // Crear tablas si no existen
    await createTables();

    // Obtener datos de la API
    const events = await fetchRouletteData();

    const validatedFields = RouletteEventsArraySchema.parse(events);

    // Crear mapa de latest_settled_at por casino
    const casinoLatestSettledMap = new Map<string, Date>();

    // Obtener mesas únicas para consultar latest_settled_at por casino
    const uniqueTableIds = new Set<string>();
    validatedFields.forEach(event => {
      uniqueTableIds.add(event.data.table.id);
    });

    // Consultar latest_settled_at para cada casino
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
    const resolvedEvents = validatedFields.filter(event => {
      if (event.data.status !== 'Resolved') return false;

      const latestSettledAt = casinoLatestSettledMap.get(event.data.table.id);

      if (!latestSettledAt) return true; // Si no hay registros previos para este casino, incluir

      const eventSettledAt = new Date(event.data.settledAt);
      return eventSettledAt > latestSettledAt;
    });

    if (resolvedEvents.length === 0) {
      return { processed: 0 };
    }

    // Cache para almacenar casino_id por table_id
    const casinoCache = new Map<string, number>();

    // Preparar datos para inserción masiva
    const rouletteEventsData: Array<{
      event_id: string;
      started_at: Date;
      settled_at: Date;
      outcome_number: number;
      outcome_type: string;
      outcome_color: string;
      casino_id: number;
    }> = [];

    // Procesar casinos primero
    const uniqueTables = new Map<string, string>();

    resolvedEvents.forEach(event => {
      const { id, name } = event.data.table;

      if (!uniqueTables.has(id)) {
        uniqueTables.set(id, name);
      }
    });

    // Obtener o crear casinos en paralelo
    const casinoPromises = Array.from(uniqueTables).map(async ([tableId, tableName]) => {
      // Buscar en la base de datos
      const casinoResult = await sql`
        SELECT id
        FROM casino
        WHERE table_id = ${tableId} AND table_name = ${tableName};
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
    resolvedEvents.forEach(event => {
      const { data } = event;
      const casinoId = casinoCache.get(data.table.id);

      if (!casinoId) {
        throw new Error("Invalid fields!")
      }

      rouletteEventsData.push({
        event_id: data.id,
        started_at: data.startedAt,
        settled_at: data.settledAt,
        outcome_number: data.result.outcome.number,
        outcome_type: data.result.outcome.type,
        outcome_color: data.result.outcome.color,
        casino_id: casinoId
      });
    });

    // Inserción masiva de eventos de roulette
    if (rouletteEventsData.length > 0) {
      await sql`
        INSERT INTO roulette_events ${
          sql(rouletteEventsData, 'event_id', 'started_at', 'settled_at', 'outcome_number', 'outcome_type', 'outcome_color', 'casino_id')
        }
        ON CONFLICT (event_id) DO NOTHING
      `;
    }

    return { processed: resolvedEvents.length };
  } catch (error) {
    console.error('Error procesando datos de roulette:', error);

    // Registrar el error en la base de datos
    if (error instanceof Error) {
      await logError(error, 'processRouletteData');
    }

    throw error;
  }
}

// Función principal
async function main() {
  try {
    console.log('🚀 Iniciando procesamiento de datos de roulette...');

    const result = await processRouletteData();

    console.log('🎉 ¡Procesamiento completado exitosamente!');
    console.log(JSON.stringify(result, null, 2));

    process.exit(0);
  } catch (error) {
    console.error('💥 Error fatal:', error instanceof Error ? error.message : String(error));
    process.exit(1);
  }
}

// Ejecutar el script
if (require.main === module) {
  main();
}