SELECT distinct
x.dt_pst::VARCHAR(1024) as dt_pst,
x.u::VARCHAR(1024) as u,
y.l::VARCHAR(1024) as l,
y.ltv::VARCHAR(1024) as ltv,
x.source::VARCHAR(1024) as source,
x.amount::VARCHAR(1024) as source_amount,
x.battleid::VARCHAR(1024) as battleid,
x.target_id::VARCHAR(1024) as target_id,
zz.defender_ext_multi::VARCHAR(1024) as defender_ext_multi,
zzz.defender_ext_extra::VARCHAR(1024) as defender_ext_extra,
zx.defender_ext_wmo::VARCHAR(1024) as defender_ext_wmo,
  
	   CASE WHEN x.source::VARCHAR(1024) = 'battle' 
			AND zzz.defender_ext_extra::VARCHAR(1024) is null 
			 AND zz.defender_ext_multi::VARCHAR(1024) is not null 
			  AND zx.defender_ext_wmo::VARCHAR(1024) is null
				   THEN zz.defender_ext_multi::VARCHAR(1024)

			WHEN x.source::VARCHAR(1024) = 'battle' 
			AND zzz.defender_ext_extra::VARCHAR(1024) is null 
			 AND zz.defender_ext_multi::VARCHAR(1024) is null 
			  AND zx.defender_ext_wmo::VARCHAR(1024) is not null
			       THEN zx.defender_ext_wmo::VARCHAR(1024)	

			WHEN x.source::VARCHAR(1024) = 'battle' 
			AND zzz.defender_ext_extra::VARCHAR(1024) is null 
			 AND zz.defender_ext_multi::VARCHAR(1024) is not null 
			  AND zx.defender_ext_wmo::VARCHAR(1024) is not null
			       THEN zx.defender_ext_wmo::VARCHAR(1024)	

			WHEN x.source::VARCHAR(1024) = 'battle' 
			AND zzz.defender_ext_extra::VARCHAR(1024) is not null 
				   THEN zzz.defender_ext_extra::VARCHAR(1024)

			WHEN x.source::VARCHAR(1024) = 'combat_victory'
				   THEN 'event'
			WHEN x.source::VARCHAR(1024) = 'event_reward'  
				   THEN 'event'
			WHEN x.source::VARCHAR(1024) in ('x1','x2','x3')
				   THEN 'event'
			WHEN x.source::VARCHAR(1024) LIKE('x4')
				   THEN 'event' 
			WHEN x.source::VARCHAR(1024) LIKE('x5')
				   THEN 'forsaken_mission'  
			ELSE x.source::VARCHAR(1024) 
				   END as macro_source  
   			   
FROM (
	SELECT distinct
	TO_CHAR(CONVERT_TIMEZONE('UTC','America/Los_Angeles', a.addtime::timestamp_ntz), 'YYYY-MM-DD') as dt_pst,
	PARSE_JSON(a.source_data):target as target_id,
	PARSE_JSON(a.source_data):battleid as battleid,
	PARSE_JSON(a.source_data):event_id as event_id,
	a.userid as u,
	a.source,
	a.amount
		 
	FROM db.user_currencies_transactions a
	WHERE amount > 0
	AND currencyid = 1
	AND source != 'cs'
	AND source != 'admin'
	AND userid not in (SELECT distinct userid FROM db.whitelist)
	AND TO_CHAR(CONVERT_TIMEZONE('UTC','America/Los_Angeles', a.addtime::timestamp_ntz), 'YYYY-MM-DD')  = '%s'
	 ) x

			LEFT OUTER JOIN (

				SELECT distinct 
				b.userid as u, 
				b.level as l, 
				b.rev_fb_primary as ltv

				FROM db.users b
				) y ON x.u::VARCHAR(1024) = y.u::VARCHAR(1024) 
 
			LEFT OUTER JOIN (

				SELECT distinct
				max(d.dt_pst) as dt_pst,
				d.src:battleid as battleid,
				f.value:userid as u,
				max(d.src:defender_ext) as defender_ext_multi

				FROM db.raw_json d, LATERAL FLATTEN(input => parse_json(d.src:attacker_level)) f
				WHERE d.tag = 'attack_multi'
				AND d.dt_pst  = '%s'
				GROUP BY d.src:battleid, f.value:userid
				) zz ON x.battleid::VARCHAR(1024) = zz.battleid::VARCHAR(1024) 
				     AND x.u::VARCHAR(1024) = zz.u::VARCHAR(1024)

			LEFT OUTER JOIN (

				SELECT distinct
				dd.dt_pst,
				dd.src:battleid as battleid,
				dd.game_user_id as u,
				dd.src:defending_fleetid as defender_ext_extra

				FROM db.raw_json dd
				WHERE dd.tag = 'attack_extra'
				AND dt_pst = '%s'
				) zzz ON x.battleid::VARCHAR(1024) = zzz.battleid::VARCHAR(1024) 
				      AND x.u::VARCHAR(1024) = zzz.u::VARCHAR(1024)

			LEFT OUTER JOIN (

			    SELECT distinct
			    e.wmid as target_id,
			    e.defender_ext as defender_ext_wmo
			   
			    FROM db.ref e
			    ) zx ON x.target_id::VARCHAR(1024) = zx.target_id::VARCHAR(1024)
