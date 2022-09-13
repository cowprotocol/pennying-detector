import logging
from duneapi.api import DuneAPI
from duneapi.types import DuneQuery, Network
import asyncio
from .util import traced
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def format_slippage_query(start_time, end_time):
    return f"""
    WITH
    filtered_batches as (
        select * from gnosis_protocol_v2."batches"
        where dex_swaps > 0 --! Exclude Purely internal settlements
        and case 
                when replace('0x', '0x', '\\x')::bytea = '\\x' then 1=1 
                else solver_address = replace('0x', '0x', '\\x')::bytea 
            end
        and block_time between '{start_time}' and '{end_time}'
    ),

    regular_transfers as (
        select 
            block_time as block_time,
            tx_hash as tx_hash,
            solver_address,
            solver_name,
            "from" sender,
            "to" receiver,
            t.contract_address as token,
            value as amount_wei
        from filtered_batches
        inner join erc20."ERC20_evt_Transfer" t
            on tx_hash = evt_tx_hash
    ),

    unwraps as (
        SELECT
            block_time,
            tx_hash,
            solver_address,
            solver_name,
            src as sender,
            decode('0000000000000000000000000000000000000000', 'hex') as receiver, --! Unwraps result in an outgoing transfer of ETH
            contract_address as token,
            wad as amount_wei
        FROM filtered_batches
        join zeroex."WETH9_evt_Withdrawal" w
            on tx_hash = evt_tx_hash
        union
        SELECT
            block_time,
            tx_hash,
            solver_address,
            solver_name,
            decode('0000000000000000000000000000000000000000', 'hex') as sender, --! Unwraps result in an outgoing transfer of ETH
            src as reveiver,
            '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as token,
            wad as amount_wei
        FROM filtered_batches
        join zeroex."WETH9_evt_Withdrawal" w
            on tx_hash = evt_tx_hash
    ),

    eth_out as (
        SELECT
            block_time,
            tx_hash,
            solver_address,
            solver_name,
            contract_address as sender,
            -- Technically this is not accurate since the trade receiver would be the recipient, 
            -- but since this is all being constructed for the contract internal balances, it doesn't matter here
            owner as receiver, 
            "buyToken" as token,
            "buyAmount" as amount_wei
        from filtered_batches
        inner join gnosis_protocol_v2."GPv2Settlement_evt_Trade"
            on tx_hash = evt_tx_hash
            and "buyToken" = '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
    ),


    batch_transfers as (
        SELECT * FROM regular_transfers
        UNION ALL --! Need to keep potential duplicates e.g. 0xf649dcf456e8b2b72f2882f223379458a25be6140159285c318e2da88e033c9d
        SELECT * FROM unwraps
        UNION ALL
        SELECT * FROM eth_out
    ),

    fees AS (
        SELECT
            tx_hash,
            block_time,
            solver_address,
            solver_name,
            "sellToken" as token,
            -1 * "feeAmount" as amount_wei --! Fees are deducted from incomming
        FROM filtered_batches
        JOIN gnosis_protocol_v2."GPv2Settlement_evt_Trade"
            ON tx_hash = evt_tx_hash
    ),

    incoming_and_outgoing as (
        SELECT 
            tx_hash,
            block_time,
            solver_address,
            solver_name,
            token,
            case 
                when receiver in (
                    '\\x3328f5f2cecaf00a2443082b657cedeaf70bfaef', -- alpha contract
                    '\\x9008D19f58AAbD9eD0D60971565AA8510560ab41' -- beta contract 
                ) then amount_wei
                when sender in (
                    '\\x3328f5f2cecaf00a2443082b657cedeaf70bfaef', -- alpha contract
                    '\\x9008D19f58AAbD9eD0D60971565AA8510560ab41' -- beta contract 
                ) then -1 * amount_wei
                -- These are trasfers due to a MultiHop routed trade 
                -- (e.g. via 1inch https://etherscan.io/tx/0xbbcd40dbf992bfa38d54a5468f75f3b5348446ecbc0089232b6b6d2acee248cd)
                else 0 
            end as amount_wei
        from batch_transfers
    ),

    tally as (
        SELECT * FROM incoming_and_outgoing
        UNION ALL
        SELECT * FROM fees
    ),

    contract_delta as (
        select 
            block_time, 
            tx_hash,
            CONCAT('0x', ENCODE(solver_address, 'hex')) as solver_address,
            solver_name,
            token,
            sum(amount_wei) as batch_delta
        from tally t
        group by block_time, tx_hash, token, solver_address, solver_name
    ),

    token_breakdown as (
        select 
            block_time,
            tx_hash,
            solver_address, 
            solver_name,
            case 
                when token = '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' then 'ETH'
                when t.symbol is not null then t.symbol 
                else TEXT(token) 
            end as symbol,
            batch_delta,
            batch_delta > 0 as is_positive,
            batch_delta / 10 ^ t.decimals * p.price as usd_value
        from contract_delta d
        left outer join prices.usd p
            on p.contract_address = case 
                when d.token = '\\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' 
                    then '\\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' 
                else d.token end
            and p.minute = date_trunc('minute', block_time)
        left outer join erc20.tokens t
            on d.token = t.contract_address
        where batch_delta != 0
        group by solver_address, solver_name, block_time, tx_hash, t.decimals, price, t.symbol, token, batch_delta
        
    ),

    results as (
        select
            block_time,
            solver_address, 
            solver_name,
            string_agg(TEXT(symbol), ',' order by symbol) as tokens_involved,
            sum(usd_value) as usd_delta,
            count(case when usd_value is null then 1 end) as num_missing_prices,
            sum(case when is_positive then 1 else 0 end) as num_positive,
            sum(case when not is_positive then 1 else 0 end) as num_negative,
            CONCAT('<a href="https://etherscan.io/tx/', CONCAT('0x', ENCODE(tx_hash, 'hex')), '" target="_blank">tx</a>') as tx_link,
            CONCAT('0x', ENCODE(tx_hash, 'hex')) as tx_hash
        from token_breakdown 
        group by solver_address, solver_name, block_time, tx_hash
    )


    select block_time as time, solver_name as solver, usd_delta as slippage from results 
    where
        usd_delta is not null
    order by usd_delta
    """

@traced(logger, 'Getting solvers slippage through Dune.')
def get_slippage(start_time, end_time):
    query = DuneQuery.from_environment(
        raw_sql=format_slippage_query(start_time, end_time),
        network=Network.MAINNET,
    )

    dune_connection = DuneAPI.new_from_environment()
    data = dune_connection.fetch(query)
    return data
