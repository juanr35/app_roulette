#!/usr/bin/env tsx

/**
 * Script para limpiar registros antiguos de roulette_events (>3 meses)
 *
 * Uso:
 * npx tsx scripts/cleanup-old-roulette-events.ts
 */

import sql from '../lib/db';

// Función para registrar errores con filtro de 24 horas
async function logError(error: Error, context?: string) {
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
      console.error('Error al insertar en error_logs:', errorMessage);
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
    console.error('❌ Error durante la limpieza de registros antiguos:', error instanceof Error ? error.message : String(error));

    // Registrar el error
    if (error instanceof Error) {
      await logError(error, 'cleanupOldRouletteEvents');
    }

    throw error;
  }
}

// Función principal
async function main() {
  try {
    console.log('🚀 Iniciando limpieza de registros antiguos de roulette...');

    const result = await cleanupOldRouletteEvents();

    console.log('🎉 ¡Limpieza completada exitosamente!');
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