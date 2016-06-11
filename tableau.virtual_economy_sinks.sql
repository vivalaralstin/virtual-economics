SELECT distinct
	x.dt_pst::VARCHAR(1024) as dt_pst,
	x.u::VARCHAR(1024) as u,
	y.l::VARCHAR(1024) as l,
	y.ltv::VARCHAR(1024) as ltv,
	x.amount::VARCHAR(1024) as amount,
	x.sink::VARCHAR(1024) as sink,
	z.category::VARCHAR(1024) as category,
	x.sink_type::VARCHAR(1024) as sink_type,
	z.asset::VARCHAR(1024) as asset,
	x.item_id::VARCHAR(1024) as item_id,
	x.item_level::VARCHAR(1024) as item_level,
	   
	CASE WHEN z.category::VARCHAR(1024) = 'research'
			THEN 'building' 
	WHEN z.category::VARCHAR(1024) = 'granted'
			THEN 'building' 		
	WHEN z.category::VARCHAR(1024) is not null
			THEN z.category::VARCHAR(1024) 							
	WHEN x.sink::VARCHAR(1024) = 'x1' 
			THEN 'x1_'							   
	WHEN z.category::VARCHAR(1024) is null 
			THEN x.sink_type::VARCHAR(1024) 
	ELSE x.sink_type::VARCHAR(1024) 
			END as macro_sink,

	CASE WHEN z.asset::VARCHAR(1024) is not null 
			THEN z.asset::VARCHAR(1024) 
	ELSE x.sink_type::VARCHAR(1024)
			END as micro_sink,
	   	  
FROM ( SELECT
       TO_CHAR(CONVERT_TIMEZONE('UTC','America/Los_Angeles', a.addtime::timestamp_ntz), 'YYYY-MM-DD') as dt_pst,
	   a.userid as u,
	   a.source as sink,
	   a.amount,
	   
		CASE WHEN a.source = 'Client Transaction'
				THEN PARSE_JSON(source_data):type
		WHEN a.source = 'Tournament'
				THEN PARSE_JSON(source_data):action
		WHEN a.source = 'x1'
				THEN a.source
		WHEN a.source = 'matchmaking_protection'
				THEN a.source
		ELSE a.source 
				END as sink_type,

		CASE WHEN a.source = 'Client Transaction'
				THEN PARSE_JSON(source_data):itemid
		WHEN a.source = 'Tournament'
				THEN NULL
		WHEN a.source = 'x1'
				THEN PARSE_JSON(source_data):crew_id
		WHEN a.source = 'matchmaking_protection'
				THEN NULL
		ELSE a.source
		END as item_id,

		CASE WHEN a.source = 'Client Transaction'
				THEN PARSE_JSON(source_data):level
		WHEN a.source = 'Tournament'
				THEN NULL
		WHEN a.source = 'x1'
				THEN NULL
		WHEN a.source = 'matchmaking_protection'
				THEN NULL
		ELSE a.source
				END as item_level

	  FROM db.user_currencies_transactions a
	  WHERE a.amount < 0
	  AND currencyid = 1
	  AND a.source != 'cs'
	  AND a.source != 'admin'
	  AND a.userid not in (SELECT distinct userid FROM db.whitelist)
	  AND TO_CHAR(CONVERT_TIMEZONE('UTC','America/Los_Angeles', a.addtime::timestamp_ntz), 'YYYY-MM-DD') = '%s'
	  ) x
 
			LEFT OUTER JOIN ( SELECT distinct 
				              b.userid as u, 
				              b.level as l, 
				              b.db.rev_prime as ltv
				              FROM db.users b
				 			  ) y ON x.u::VARCHAR(1024) = y.u::VARCHAR(1024) 

			LEFT OUTER JOIN ( SELECT distinct 
				              c.id, c.name, 
				              c.componenttype as component_type, 
				              c.category,
				  
							  CASE WHEN c.name = 'y1' THEN 'y1_'
							        WHEN c.name = 'y2' THEN 'y2_'
							         WHEN c.name = 'y3' THEN 'y3_'
							          WHEN c.name = 'y4' THEN 'y4'
							           ELSE c.name END as Asset
				 
				             FROM db.blueprints c
				             ) z ON x.item_id::VARCHAR(1024) = z.id::VARCHAR(1024)
 
GROUP BY  x.u,
          y.l,
          x.dt_pst,
	      y.ltv,
		  x.amount,
	      x.sink,
		  x.sink_type,
		  z.category,
		  z.asset,
	      x.item_id,
	      x.item_level,
	      x.crew_type,
	      x.crew_action,
		  macro_sink,
		  micro_sink