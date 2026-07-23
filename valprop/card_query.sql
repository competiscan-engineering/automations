-- =====================================================================================
-- COSAS QUE TENEMOS QUE AGREGAR PA SIGUIENTE VERSION:
-- 1) Lista completa de tarjetas 
-- 2) FICO/risk profiles
-- 3) Full months needed. (Feb-July)
-- =====================================================================================

SELECT
    p.product_name                                   AS `product_name`,
    p.entry_id                                        AS `entry_id`,
    p.mtype_id                                        AS `mtype_id`,

    company_agg.primary_company                       AS `primary_company`,

    mc.mChannelName                                    AS `Media Channel`,

    p.mtype_id                                        AS `mtype_id`,
    mt.mTypeName                                       AS `Mailing Type`,

    pp.ppdate                                          AS `ppdate`,

    -- Annual fees
    pc.Tier1AnnualFee                                  AS `Payment Cards - Tier 1 Annual Fee ($)`,
    pc.Tier2AnnualFee                                  AS `Payment Cards - Tier 2 Annual Fee ($)`,
    NULL                                                AS `Payment Cards - Tier 3 Annual Fee ($)`,  -- este no existe/no lo encuentra

    -- Application type (resolved via cscan_application_type; raw ID kept)
    at.ApplicationTypeName                             AS `Application Type`,
    pc.ApplicationType                                 AS `Application Type ID (Raw)`,

    -- Purchase regular APR
    pc.PurchaseRegularAPR                              AS `Payment Cards - Tier 1 Purchase Regular APR (%)`,
    pc.Tier2PurchaseRegularAPR                         AS `Payment Cards - Tier 2 Purchase Regular APR (%)`,
    pc.Tier3PurchaseRegularAPR                         AS `Payment Cards - Tier 3 Purchase Regular APR (%)`,

    -- Purchase introductory offer
    pc.PurchaseIntroductoryAPR                         AS `Payment Cards - Purchase Introductory APR (%)`,
    pc.PurchaseIntroductoryPeriod                      AS `Payment Cards - Purchase Introductory Period (Months)`,

    -- Rewards
    pc.RewardsProgramEmphasis                          AS `Payment Cards - Rewards Program Emphasis`,
    rt.RewardTypeName                                  AS `Rewards Program`,
    pc.RewardsProgram                                  AS `Rewards Program ID (Raw)`,

    -- Balance-transfer introductory offer
    pc.BalanceTransferIntroductoryAPR                  AS `Payment Cards - Balance Transfer Introductory APR (%)`,
    pc.BalanceTransferIntroductoryPeriod               AS `Payment Cards - Balance Transfer Introductory Period (Months)`,

    -- Balance-transfer regular APR
    pc.BalanceTransferRegularAPR                       AS `Payment Cards - Tier 1 Balance Transfer Regular APR (%)`,
    pc.Tier2BalanceTransferRegularAPR                  AS `Payment Cards - Tier 2 Balance Transfer Regular APR (%)`,
    pc.Tier3BalanceTransferRegularAPR                  AS `Payment Cards - Tier 3 Balance Transfer Regular APR (%)`,

    -- Late fees
    pc.Tier1LateFee                                    AS `Payment Cards - Tier 1 Late Fee ($)`,
    pc.Tier2LateFee                                    AS `Payment Cards - Tier 2 Late Fee ($)`,
    NULL                                                AS `Payment Cards - Tier 3 Late Fee ($)`,     -- Tier3LateFee does not exist/no lo encuentra
    pc.LateFee                                         AS `Payment Cards - Late Fee (Non-Tiered, Diagnostic) ($)`,

    -- Other fees / APR fields
    pc.Tier1OverlimitFee                               AS `Tier1OverlimitFee`,
    pc.PenaltyAPR                                      AS `Payment Cards - Penalty APR (%)`,
    pc.PenaltyAPRDetails                               AS `Payment Cards - Penalty APR Details`,
    pc.StandardMonthlyMaintenanceFee                   AS `Payment Cards - Standard Monthly Maintenance Fee ($)`,

    -- Sign-on incentive (proxy: incentive_signon populated -- see assumption #4 above)
    incentive_agg.incentive_signon                     AS `Sign-On Incentive`,
    incentive_agg.max_award                            AS `Sign-On Max award`,
    incentive_agg.window1                              AS `Sign-On Incentive Window (months)`

FROM csv2_product.cscan_product p

-- Primary company: pre-aggregated so multiple default_img=1 rows never multiply product rows.
LEFT JOIN (
    SELECT
        pcm.product_id,
        GROUP_CONCAT(DISTINCT c.companyName ORDER BY c.companyName SEPARATOR '||') AS primary_company
    FROM csv2_product.cscan_product_company_mapping pcm
    INNER JOIN csv2_master_lookup.cscan_company c
        ON c.companyID = pcm.company_id
    WHERE pcm.default_img = 1
    GROUP BY pcm.product_id
) company_agg
    ON company_agg.product_id = p.product_id

LEFT JOIN csv2_master_lookup.cscan_mchannel mc
    ON mc.mChannelID = p.mchanne_id

LEFT JOIN csv2_master_lookup.cscan_mtype mt
    ON mt.mTypeID = p.mtype_id

-- Panelist date: pre-aggregated to the most recent ppdate per product, restricted to
-- July 2026 (for test). 
INNER JOIN (
    SELECT
        productID,
        MAX(ppdate) AS ppdate
    FROM csv2_product.cscan_panelists_product
    WHERE ppdate >= '2026-07-01 00:00:00'
      AND ppdate <  '2026-08-01 00:00:00'
    GROUP BY productID
) pp
    ON pp.productID = p.product_id


LEFT JOIN csv2_product.cscan_payment_cards pc
    ON pc.productID = p.product_id

-- Application type / rewards program labels: each is a simple PK lookup (one row per
-- ID)
LEFT JOIN csv2_master_lookup.cscan_application_type at
    ON at.ApplicationTypeID = pc.ApplicationType

LEFT JOIN csv2_master_lookup.cscan_reward_type rt
    ON rt.RewardTypeID = pc.RewardsProgram

-- Sign-on incentive: pre-aggregated because cscan_product_incentive_type_mapping can
-- have multiple rows per product
LEFT JOIN (
    SELECT
        product_id,
        GROUP_CONCAT(DISTINCT incentive_signon ORDER BY incentive_signon SEPARATOR '||') AS incentive_signon,
        MAX(max_award) AS max_award,
        MAX(window1)   AS window1
    FROM csv2_product.cscan_product_incentive_type_mapping
    WHERE incentive_signon IS NOT NULL
      AND incentive_signon <> ''
    GROUP BY product_id
) incentive_agg
    ON incentive_agg.product_id = p.product_id

WHERE p.product_name IN (
    'AAA Cashback Visa Signature Card',
    'AAA Daily Advantage Visa Signature Credit Card',
    'AAA Dollars Plus Visa Signature Card',
    'AAA Travel Advantage Visa Signature Credit Card',
    'AARP Essential Rewards Mastercard',
    'AARP Travel Rewards Mastercard',
    'Academy Sports + Outdoors Credit Card',
    'Ally Everyday Cash Back Mastercard',
    'Ally Platinum Mastercard',
    'Ally Unlimited Cash Back for Nurses and Educators Credit Card',
    'Ally Unlimited Cash Back Mastercard',
    'Alterna Savings Visa Infinite Card',
    'Amazon Business Prime American Express Card',
    'Amazon Prime Rewards Visa Card',
    'Amazon Secured Card',
    'Amazon.com Store Card',
    'American Airlines AAdvantage MileUp Card',
    'American Express Gold Card',
    'Ann Taylor Credit Card',
    'Ann Taylor Mastercard',
    'Apple Card',
    'Ashley Advantage Synchrony Credit Card',
    'Aspire Mastercard',
    'At Home Insider Perks Credit Card',
    'At Home Insider Perks Mastercard',
    'AT&T Points Plus Card from Citi',
    'Athleta Rewards Credit Card',
    'Athleta Rewards Mastercard',
    'Atmos Rewards Ascent Visa Signature Card',
    'Atmos Rewards Summit Visa Infinite Card',
    'Atmos Rewards Visa Signature Business Card',
    'Avant Mastercard',
    'Banana Republic Rewards Credit Card',
    'Banana Republic Rewards Mastercard',
    'Bank of America Business Advantage Customized Cash Rewards Credit Card',
    'Bank of America Business Advantage Travel Rewards Credit Card',
    'Bank of America Cash Rewards Secured Card',
    'Bank of America Premium Rewards Credit Card',
    'Bank of America Premium Rewards Elite Credit Card',
    'Bank of America Travel Rewards Credit Card',
    'Bank of America Unlimited Cash Rewards Credit Card',
    'BankAmericard Credit Card',
    'BankAmericard Secured Credit Card',
    'Bass Pro Shops and Cabela''s CLUB Business card',
    'BCU Cash Rewards Visa Card',
    'BCU Cash Rewards Visa Signature Card',
    'BCU Simply Visa Credit Card',
    'BECU Business Visa with Cash Rewards',
    'BECU Visa Credit Card',
    'Belk Credit Card',
    'Belk Rewards+ Mastercard',
    'Best Buy Business Advantage Card',
    'Best Western Rewards Premium Visa Signature Card',
    'Best Western Rewards Visa Signature Card',
    'Bilt Mastercard',
    'BJ''s One Mastercard',
    'BJ''s One+ Mastercard',
    'Blaze Mastercard',
    'Bloomingdale''s American Express Card',
    'Bloomingdale''s Credit Card',
    'BMO Ascend World Elite Mastercard',
    'BMO CashBack Business Mastercard',
    'BMO CashBack Mastercard',
    'BMO CashBack World Elite Mastercard',
    'BMO eclipse Visa Infinite Card',
    'BMO Preferred Rate Mastercard',
    'BMW Card',
    'Booking.com Genius Rewards Visa Signature Credit Card',
    'BPme Rewards Visa Card',
    'BrandsMart USA Credit Card',
    'Bread Cashback American Express Credit Card',
    'Burlington Credit Card',
    'Capital One Platinum Card',
    'Capital One QuicksilverOne Rewards Credit Card',
    'Capital One Savor Cash Rewards Credit Card',
    'Capital One Spark 1.5% Cash Select Mastercard',
    'Capital One Venture Card',
    'Capital One VentureOne Visa Credit Card',
    'Capital One Walmart Rewards Card',
    'Capital One Walmart Rewards World Mastercard',
    'Carnival World Mastercard',
    'Carter''s Credit Card',
    'cashRewards Card',
    'Cathay Pacific Visa Signature Card',
    'Cathay Pacific World Elite Mastercard',
    'Chase Freedom Flex Credit Card',
    'Chase Freedom Rise Credit Card',
    'Chase Freedom Unlimited',
    'Chase Sapphire Preferred',
    'Chase Sapphire Reserve',
    'Chase Slate',
    'Chase Slate Edge Visa Platinum Card',
    'CheapOair Visa Credit Card',
    'CheapOair Visa Signature Credit Card',
    'Chevron and Texaco Techron Advantage Credit Card',
    'Chevron and Texaco Techron Advantage Visa Gold Card',
    'Chime Credit Builder Secured Visa Credit Card',
    'Choice Privileges Mastercard',
    'Choice Privileges Select Mastercard',
    'Chrysler DrivePlus Mastercard',
    'CIBC Aventura Visa Card for Business',
    'CIBC Aventura Visa Infinite Card',
    'CIBC Dividend Visa Card',
    'CIBC Dividend Visa Infinite Card',
    'CIBC Select Visa Card',
    'CITGO Rewards Card',
    'Citi / AAdvantage Business World Elite Mastercard',
    'Citi / AAdvantage Executive World Elite Mastercard',
    'Citi Custom Cash Card',
    'Citi Diamond Preferred',
    'Citi Double Cash Card',
    'Citi Simplicity Card',
    'Citi Strata Card',
    'Citi Strata Premier Card',
    'Citizens Summit World Mastercard',
    'Comenity Card Mastercard',
    'Costco Anywhere Visa Business Card by Citi',
    'Costco Anywhere Visa Card',
    'Crate and Barrel Visa Signature Card',
    'Credit One Bank Platinum Rewards Visa',
    'Credit One Bank Platinum Visa Card',
    'Credit One Bank Platinum X5 Visa Card',
    'Credit One Bank Premier American Express Card',
    'Delta SkyMiles Gold Mastercard',
    'Delta SkyMiles Reserve American Express Card',
    'Delta SkyMiles Reserve Business American Express Card',
    'Desjardins Cash Back Visa card',
    'Dillard''s Credit Card',
    'Dillard''s Mastercard',
    'Dillons Rewards World Elite Mastercard',
    'Discover it Cash Back Credit Card',
    'Discover it Miles',
    'Discover it Secured',
    'Discover it Student Cash Back Card',
    'Disney Premier Visa Card',
    'Disney Visa Card',
    'DoorDash Rewards Mastercard',
    'eBay Mastercard',
    'ExxonMobil Smart Card',
    'FanCash Rewards Card',
    'Fidelity Rewards Visa Signature Card',
    'Fifth Third 1% Cash/Back Credit Card',
    'Fifth Third 1.67% Cash/Back Card',
    'My Best Buy Credit Card',
    'My Best Buy Visa Card',
    'NFL Extra Points Visa Signature Card',
    'The Children''s Place Credit Account',
    'Phillips 66-Conoco-76 Credit Card'
);
