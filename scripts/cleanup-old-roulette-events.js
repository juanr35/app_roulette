#!/usr/bin/env node

/**
 * Script para limpiar registros antiguos de roulette_events (>3 meses)
 *
 * Uso:
 * node scripts/cleanup-old-roulette-events.js
 */

// Configuración desde variables de entorno
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error('❌ Error: DATABASE_URL no está configurada');
  process.exit(1);
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
      VALUES (${errorMessage}, ${errorStack}, ${context || 'cleanupOldRouletteEvents'})
    `.catch(() => {
      // Si falla la inserción, al menos loguear en consola
      console.error('Error al insertar en error_logs:', error);
    });

  } catch (logError) {
    // Si falla el logging, al menos loguear en consola
    console.error('Error al registrar error en base de datos:', logError);
  }
}

// Función para limpiar registros antiguos de roulette_events (>3 meses)
async function cleanupOldRouletteEvents() {
  try {
    console.log('🧹 Iniciando limpieza de registros antiguos...');

    // Calcular fecha de 3 meses atrás
    const threeMonthsAgo = new Date();
    threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);

    console.log(`📅 Eliminando registros anteriores a: ${threeMonthsAgo.toISOString()}`);

    // Eliminar registros antiguos
    const deleteResult = await sql`
      DELETE FROM roulette_events
      WHERE created_at < ${threeMonthsAgo}
    `;

    const deletedCount = deleteResult.length;

    console.log(`✅ Limpieza completada: ${deletedCount} registros antiguos eliminados`);

    return { deleted: deletedCount };
  } catch (error) {
    console.error('❌ Error durante la limpieza de registros antiguos:', error.message);

    // Registrar el error
    await logError(sql, error, 'cleanupOldRouletteEvents');

    throw error;
  }
}

// Función principal
async function main() {
  const sql = createPostgresConnection();

  try {
    console.log('🚀 Iniciando limpieza de registros antiguos de roulette...');

    const result = await cleanupOldRouletteEvents();

    console.log('🎉 ¡Limpieza completada exitosamente!');
    console.log(JSON.stringify(result, null, 2));

    process.exit(0);
  } catch (error) {
    console.error('💥 Error fatal:', error.message);
    process.exit(1);
  } finally {
    await sql.end();
  }
}

// Ejecutar el script
if (require.main === module) {
  main();
}