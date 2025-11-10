import * as z from "zod";

const RouletteOutcomeSchema = z.object({
	number: z.number(),
  type: z.literal(["Even", "Odd"]),
	color: z.literal(["Red", "Black", "Green"])
});

const RouletteTableSchema = z.object({
	id: z.string(),
	name: z.string()
});

const RouletteDataSchema = z.object({
	id: z.string(),
	startedAt: z.coerce.date(),
	settledAt: z.coerce.date(),
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

export const RouletteEventSchema = z.object({
	id: z.string(),
	data: RouletteDataSchema
});