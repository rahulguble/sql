    
    dim_sub =
        select client
                , sales_document
                ,sales_document_item
                ,dim_sub.created_date_item              
                ,dim_sub.payment_method 
                ,dim_sub.contract_start_date_bp
                ,dim_sub.contract_end_date_bp
                ,dim_sub.sales_document_type
                ,dim_sub.subscription_category
                ,dim_sub.offer_type
                ,(case when dim_sub.offer_type = sub_param.param_val THEN 'Y' else 'N' end) trial_order_flag
                ,dim_sub.route_to_market
                ,dim_sub.billing_frequency
                ,dim_sub.bill_plan_number
        from    TRNFRM_S4H_CCM.dim_subscription  dim_sub
        left outer join TRNFRM_S4H_CCM.SUBSCRIPTION_PARAMS sub_param on (sub_param.param_val = dim_sub.offer_type and sub_param.param_categ = 'TWP_OFFERS' and sub_param.param_name = 'OFFER_TYPE')
        where    dim_sub.subscription_category =st_subscription_category                
                and dim_sub.subscription_type = 'IN'
                and dim_sub.sales_document_type = 'ZCSB';   
                
   
    -- Get the bill plan  records from table dim_billing_plan for all CC Individual subscription documents based on cut off date and current_date
    delta_fplt = select 
                    dim_sub.client as client
                    ,dim_sub.sales_document as sales_document
                    ,dim_sub.sales_document_item as sales_document_item
                    ,dim_bp.fplnr bp_no
                    ,dim_bp.fpltr bp_item               
                    ,(CASE WHEN dim_bp.ofkdat = '00000000' OR dim_bp.ofkdat > dim_bp.fkdat THEN dim_bp.fkdat ELSE dim_bp.ofkdat END)  as bp_start_date                 
                    ,dim_bp.nfdat as bp_end_date 
                    ,dim_bp.faksp as bp_billing_block 
                    ,dim_bp.netpr as bp_net_price
                    ,dim_bp.upd_ind as bp_upd_ind
					,coalesce(prev_bp.upd_ind,'') as prev_upd_ind											 
                    --,dim_bp.fpart as bp_fpart
                    --,dim_bp.perio as bp_perio
                from  REPLICN_S4H.FPLT dim_bp -- TRNFRM_S4H_CCM.dim_billing_plan dim_bp  
                inner join :dim_sub dim_sub on (dim_bp.fplnr = dim_sub.bill_plan_number and (dim_bp.ofkdat <> '00000000' OR dim_bp.fkdat <> '00000000'))
				        left join REPLICN_S4H.FPLT prev_bp on (dim_bp.fplnr = prev_bp.fplnr
                	  and to_nvarchar(add_days(prev_bp.nfdat,1),'YYYYMMDD') = (CASE WHEN dim_bp.ofkdat = '00000000' OR dim_bp.ofkdat > dim_bp.fkdat THEN dim_bp.fkdat ELSE dim_bp.ofkdat END))																   
                where 
                     (dim_bp.ofkdat between ts_cutoff_date_sap and ts_current_date_sap              
                    or (dim_bp.ofkdat <= ts_cutoff_date_scd_sap and dim_bp.nfdat >= ts_cutoff_date_scd_sap)
                    or (dim_bp.ofkdat <= ts_current_date_sap and dim_bp.nfdat >= ts_current_date_sap)
                    )
					 and coalesce(prev_bp.upd_ind,'') <> 'REVP'										  
                    --or dim_bp.nfdat between  ts_cutoff_date_scd_sap and ts_yesterday_sap)


  union

                select 
                    dim_sub.client as client
                    ,dim_sub.sales_document as sales_document
                    ,dim_sub.sales_document_item as sales_document_item
                    ,dim_bp.fplnr bp_no
                    ,dim_bp.fpltr bp_item               
                    ,ZCC04.procdate  as bp_start_date                 
                    ,dim_bp.nfdat as bp_end_date 
                    ,dim_bp.faksp as bp_billing_block 
                    ,dim_bp.netpr as bp_net_price
                    ,coalesce(dim_bp.upd_ind,'') as bp_upd_ind
                    ,coalesce(prev_bp.upd_ind,'') as prev_upd_ind
                    from REPLICN_S4H.ZCC04 ZCC04
                    join replicn_S4H.vbrp vbrp on (zcc04.vbeln = vbrp.aubel AND zcc04.refdoc = vbrp.vbeln)
                    join REPLICN_S4H.FPLT dim_bp on (dim_bp.fplnr = vbrp.fplnr and dim_bp.fpltr = vbrp.fpltr)
                    inner join :dim_sub dim_sub on (dim_bp.fplnr = dim_sub.bill_plan_number and (dim_bp.ofkdat <> '00000000' OR dim_bp.fkdat <> '00000000'))
                    left join REPLICN_S4H.FPLT prev_bp on (dim_bp.fplnr = prev_bp.fplnr
                	  and to_nvarchar(add_days(prev_bp.nfdat,1),'YYYYMMDD') = (CASE WHEN dim_bp.ofkdat = '00000000' OR dim_bp.ofkdat > dim_bp.fkdat THEN dim_bp.fkdat ELSE dim_bp.ofkdat END))
                    where zcc04.transtat = 'SUCCESS'
                    and coalesce(prev_bp.upd_ind,'') = 'REVP'
                    and ZCC04.procdate between ts_cutoff_date_scd_sap and ts_current_date_sap
    ;
        
      
    --Find the billing documents for selected bill plan records which has a bill block
    bill_docs = select 
                    vbrp.VBELN as billing_document
                    ,vbrp.POSNR as billing_document_item    
                    ,vbrp.AUBEL as sales_document
                    ,vbrp.AUPOS as sales_document_item
                    ,vbrp.FPLNR as bp_no
                    ,vbrp.FPLTR as bp_item  
                    ,to_date(vbrp.erdat,'YYYYMMDD') as created_date 
                from REPLICN_S4H.vbrp as vbrp
                inner join REPLICN_S4H.vbrk vbrk on
                    vbrk.mandt = vbrp.mandt
                    and vbrk.vbeln = vbrp.vbeln
                inner join :delta_fplt on
                    :delta_fplt.client = vbrp.mandt
                    and :delta_fplt.bp_no = vbrp.fplnr
                    and :delta_fplt.bp_item = vbrp.fpltr 
                where vbrk.fkart = 'F2'
    ;
    -- Find the billing documents for selected bill plan records which has a bill block and not matching with bp item number
    bill_docs2 = select 
                    vbrp.VBELN as billing_document
                    ,vbrp.POSNR as billing_document_item    
                    ,vbrp.AUBEL as sales_document
                    ,vbrp.AUPOS as sales_document_item
                    ,vbrp.FPLNR as bp_no
                    ,to_date(VBRP.FBUDA,'YYYYMMDD') as bp_end_date
                    ,to_date(vbrp.erdat,'YYYYMMDD') as created_date
                from REPLICN_S4H.vbrp as vbrp
                inner join REPLICN_S4H.vbrk vbrk on
                    vbrk.mandt = vbrp.mandt
                    and vbrk.vbeln = vbrp.vbeln
                inner join :delta_fplt on
                    :delta_fplt.client = vbrp.mandt
                    and :delta_fplt.bp_no = vbrp.fplnr
                    and :delta_fplt.bp_item <> vbrp.fpltr
                    and :delta_fplt.bp_end_date = to_date(vbrp.fbuda,'YYYYMMDD')    
                where vbrk.fkart = 'F2'
    ;
    -- Find the billing document records for all sales_document with S3 on it during the processing period
    bill_docs_s3 =  select
                        vbrp.mandt as client
                        ,vbrp.VBELN as billing_document
                        ,vbrp.POSNR as billing_document_item    
                        ,vbrp.AUBEL as sales_document
                        ,vbrp.AUPOS as sales_document_item
                        ,vbrp.FPLNR as bp_no
                        ,vbrp.FPLTR as bp_item
                        ,to_date(VBRP.FBUDA,'YYYYMMDD') as bp_end_date
                        ,vbrp.erdat as created_date 
                from REPLICN_S4H.vbrp as vbrp
                inner join REPLICN_S4H.vbrk as vbrk on
                    vbrk.mandt = vbrp.mandt
                    and vbrk.vbeln = vbrp.vbeln
                inner join TRNFRM_S4H_CCM.scd_sales_document scd_sd 
                    on scd_sd.sales_document = vbrp.aubel
                    and scd_sd.sales_document_item = vbrp.aupos
                inner join :dim_sub dim_sub
                    on dim_sub.sales_document = scd_sd.sales_document 
                    and dim_sub.sales_document_item = scd_sd.sales_document_item
                where 
                    scd_sd.cancellation_reason in ( 'S3','SC') -- S3- Suspended 30 AND SC- grace end        
                and vbrp.erdat between ts_cutoff_date_sap and ts_current_date_sap 
                and vbrk.fkart = 'F2'  --invoice
    ;       
    --  Get  credit  memo  for  all  the  billing  frequencies  from  Invoice  table.
    bill_docs_g2 = select
                        vbrp.mandt as client
                        ,vbrp.VBELN as billing_document
                        ,vbrp.POSNR as billing_document_item    
                        ,dim_sub.sales_document as sales_document
                        ,dim_sub.sales_document_item sales_document_item 
                        ,vbrp.erdat as created_date
                        ,dim_sub.billing_frequency as billing_frequency
                        ,vbak.augru as order_reason
                from REPLICN_S4H.vbrp as vbrp
                inner join REPLICN_S4H.vbrk as vbrk on
                    vbrk.mandt = vbrp.mandt
                    and vbrk.vbeln = vbrp.vbeln         
                inner join REPLICN_S4H.vbap vbap on
                    vbap.mandt = vbrp.mandt
                    and vbap.vbeln = vbrp.aubel
                    and vbap.posnr = vbrp.aupos                 
                inner join :dim_sub dim_sub on
                        dim_sub.client = vbap.mandt
                    and dim_sub.sales_document = vbap.vgbel
                    and dim_sub.sales_document_item = vbap.vgpos       
                 -- inner join REPLICN_S4H.vbkd as vbkd on
                 --  vbkd.mandt = dim_sub.client
                 --  and vbkd.vbeln = dim_sub.sales_document
                 --  and vbkd.posnr = dim_sub.sales_document_item                    
                --inner join REPLICN_S4H.fpla fpla on
                --   fpla.mandt = vbkd.mandt
                --   and fpla.fplnr = vbkd.fplnr     
                inner join REPLICN_S4H.vbak vbak on
                    vbak.mandt = vbap.mandt
                    and vbak.vbeln = vbrp.aubel                   
                where 
              --    fpla.perio = 'YA' and fpla.fpart = 'YA'
                vbrp.erdat between ts_cutoff_date_sap and ts_current_date_sap 
                and vbrk.fkart = 'G2' --G2 credit memo
    ;            

     
     --Union all SCD/Event records together 
    delta_scd_sd_all = 
    --Relevant SCD records from Sales Document SCD
                select
                    scd_sd.client   
                    ,scd_sd.sales_document
                    ,scd_sd.sales_document_item
                    ,scd_sd.start_date
                    ,null end_date
                    ,scd_sd.billing_block_header
                    ,scd_sd.billing_block_item
                    ,scd_sd.cancellation_reason
                    ,scd_sd.cancellation_reason_old
                    ,scd_sd.reason_for_rejection
                    ,scd_sd.subscription_quantity                  
                    ,scd_sd.created_date  as created_date
                    ,scd_sd.first_pmt_success_date            
                from TRNFRM_S4H_CCM.scd_sales_document scd_sd         
                inner join :dim_sub dim_sub on
                    dim_sub.client = scd_sd.client
                    and dim_sub.sales_document = scd_sd.sales_document
                    and dim_sub.sales_document_item = scd_sd.sales_document_item                                
                where  scd_sd.start_date between ts_cutoff_date and current_date
       ;
    --Merge bill plan data with SCD records
    -- Use a temp table to store all events to process. This to improve the performance
    --delete from TRNFRM_S4H_CCM.TMP_DELTA_SCD_SD_BP;
    --insert into TRNFRM_S4H_CCM.TMP_DELTA_SCD_SD_BP
    TMP_DELTA_SCD_SD_BP = select 
                            scd_sd.client   
                            ,scd_sd.sales_document
                            ,scd_sd.sales_document_item
                            ,scd_sd.start_date
                            ,null end_date
                            ,scd_sd.billing_block_header
                            ,scd_sd.billing_block_item
                            ,scd_sd.cancellation_reason                 
                            ,scd_sd.cancellation_reason_old                 
                            ,scd_sd.reason_for_rejection
                            ,scd_sd.subscription_quantity                                   
                            ,scd_sd.created_date  as created_date
                            ,fplt.bp_no as bp_no
                            ,fplt.bp_item as bp_item
                            ,fplt.bp_start_date as bp_start_date
                            ,fplt.bp_end_date as bp_end_date
                            ,fplt.bp_billing_block as bp_billing_block              
                            ,fplt.bp_net_price as bp_net_price
                            --,fplt.bp_fpart as bp_fpart
                            --,fplt.bp_perio as bp_perio
                            ,scd_sd.first_pmt_success_date
                            ,fplt.bp_upd_ind
                        from :delta_scd_sd_all scd_sd
                        left outer join  :delta_fplt fplt
                             on fplt.client = scd_sd.client
                             and fplt.sales_document = scd_sd.sales_document
                             and fplt.sales_document_item = scd_sd.sales_document_item
                             and scd_sd.start_date between fplt.bp_start_date and fplt.bp_end_date;
    tmp_bill_doc =
               select distinct
                    scd_sd.client
                    ,scd_sd.sales_document
                    ,scd_sd.sales_document_item
                    ,scd_sd.start_date
                from :TMP_DELTA_SCD_SD_BP scd_sd
                 inner join :bill_docs bill_doc2
                    on  bill_doc2.sales_document = scd_sd.sales_document
                and    bill_doc2.sales_document_item = scd_sd.sales_document_item 
                and    bill_doc2.bp_no = scd_sd.bp_no
                and    bill_doc2.bp_item = scd_sd.bp_item       
                and    bill_doc2.created_date <= scd_sd.start_date  ;
                
    tmp_bill_doc2 =
               select distinct
                    scd_sd.client
                    ,scd_sd.sales_document
                    ,scd_sd.sales_document_item
                    ,scd_sd.start_date
                from :TMP_DELTA_SCD_SD_BP scd_sd
                 inner join :bill_docs2 bill_doc2
                    on  bill_doc2.sales_document = scd_sd.sales_document
                and    bill_doc2.sales_document_item = scd_sd.sales_document_item 
                and    bill_doc2.bp_no = scd_sd.bp_no   
                and    bill_doc2.bp_end_date = scd_sd.bp_end_date   
                and    bill_doc2.created_date <= scd_sd.start_date  ;       
                    
    -- Derive the subscription status ACTIVE/INACTIVE for each of SCD records based on the business logic. Also decide if it is inactive on the day when it was created.
                            select 
                            scd_sd.client client
                            ,scd_sd.sales_document sales_document
                            ,scd_sd.sales_document_item sales_document_item
                            ,scd_sd.start_date start_date
                            ,null end_date
                            ,case 
                            -- Marking document as Inactive  when  Revival  is  done  in  mid  of  billing  cycle.   
			                         when scd_sd.bp_upd_ind = 'REVP' then 'INACTIVE'
                            -- Contract start date is greater than created date
                                when scd_sd.start_date < dim_sub.contract_start_date_bp
                                        then 'INACTIVE'     
                            -- Contract end date
                                when scd_sd.start_date > dim_sub.contract_end_date_bp
                                    then 'INACTIVE'  
                            -- Billing Block Header         
                                when (scd_sd.billing_block_header <> '' and scd_sd.billing_block_header is not NULL) 
                                        then 'INACTIVE'                 
                            -- Billing Block Item           
                                when (scd_sd.billing_block_item <> '' and scd_sd.billing_block_item is not NULL) 
                                        then 'INACTIVE'
                            -- Making document Inactive when SC is received along with FPLT-UPD_IND = 'RTNE' 
                                when scd_sd.cancellation_reason = 'SC' and scd_sd.bp_upd_ind = 'RTNE'
                                        then 'INACTIVE'
                            -- Marking document as Inactive when it goes to Suspended (SA) state on the same day of bill plan start date of current Bill Plan - to take care of February Month Shorten Bill Plan Issue 
                                --when scd_sd.cancellation_reason = 'SA' and (coalesce(scd_sd.bp_fpart,'') <> 'YA' or coalesce(scd_sd.bp_perio,'') <> 'YA') and scd_sd.start_date = scd_sd.bp_start_date
                                when scd_sd.cancellation_reason = 'SA' and dim_sub.billing_frequency <> 'PUF' and scd_sd.start_date = scd_sd.bp_start_date
                                        then 'INACTIVE'                 
                            -- Cancellation Reason  S2, Z8, INACTIVE on the event date      
                            --  'HC' = Customer Cancel=closed, S2 = Closed, 'HD' = Plan Change (Hard Cancel) = CLosed, 'HC' = Customer cancel, 'HE' = Save Offer 'HA' = closed from payment failure = CLosed, 'SD' = Plan Change (Soft Cancel) 'SF' = Expired 
                                when scd_sd.cancellation_reason in ('S2','Z8', 'HC', 'HD', 'HE', 'HA', 'SD', 'SF','HL','HP')
                                        then 'INACTIVE'                 
                            -- Reason for rejection INACTIVE on the event date      
                            -- RFC 'HB' = Replacement SKU (Hard Cancel) , 'S5' = End of commitment 
                            -- RFR 'S4' = Closed
                                when ((scd_sd.reason_for_rejection <> '' and scd_sd.reason_for_rejection is not NULL 
                                    and scd_sd.reason_for_rejection <> 'S4') 
                            -- Marking document as Inactive  when Reason_for_rejection  is  S4.  
                                    or ( scd_sd.reason_for_rejection  = 'S4' and scd_sd.cancellation_reason not in ('S5','HB') ) )                          
                                        then 'INACTIVE' 
										
                            -- Net price 0 
			    -- Changes start 100% discount for FLEX PROMO
                            -- when dim_sub.subscription_category <> 'SERV' and vbak.AUGRU NOT IN ('911', '912', '913', '921') and 
                            -- scd_sd.bp_net_price = 0   
                            -- then 'INACTIVE'
                            -- Changes end 100% discount for FLEX PROMO
							
                            -- bp_net_price is null (no bp available)               
                                when dim_sub.subscription_category <> 'SERV' and vbak.AUGRU NOT IN ('911', '912', '913','921') and scd_sd.bp_net_price is null
                                        then 'INACTIVE'     
                            --Prepaid Upfront transactions, mark it as inactive whenever cancellation_reason = 'S3'
                            	--when scd_sd.cancellation_reason in ( 'S3','SC')  and ((scd_sd.bp_fpart = 'YA' and scd_sd.bp_perio = 'YA') or vbak.AUGRU IN ('921'))
                                when scd_sd.cancellation_reason in ( 'S3','SC')  and (dim_sub.billing_frequency = 'PUF' or vbak.AUGRU IN ('921'))
                                        then 'INACTIVE' 
                            --  Prepaid Upfront transactions, mark it as inactive on the day credit memo is created
                                --when scd_sd.bp_fpart = 'YA' and scd_sd.bp_perio = 'YA'
                                when dim_sub.billing_frequency = 'PUF' 
                                    and credit_memo.sales_document is not null
                                        then 'INACTIVE'    
                        	-- APM/M2M transactions, if  cancellation_reason is 'NB',  mark it as inactive on the day credit memo is issued  
                        	    when scd_sd.cancellation_reason = 'NB' and dim_sub.billing_frequency <> 'PUF' and credit_memo.order_reason in ('SCC','HCC','915')  
                        	    		then 'INACTIVE'                     
                            -- Direct Debit first month cancellation due to payment failure 
                            -- S3 = Suspended30 S4 = Stopped 
                            -- SC = Grace End = Suspended30     SB = Customer Stop = Stopped                            
                                when dim_sub.payment_method in ( '06','12') and scd_sd.cancellation_reason in ( 'S3','SC') 
                                    and scd_sd.cancellation_reason_old not in ( 'S4','SB') and dim_sub.contract_start_date_bp = scd_sd.bp_start_date
                                        then 'INACTIVE'                 
                            -- Cancellation Reason  S3 INACTIVE on next bill plan date and also check bill plan billing block                                           
                                when scd_sd.cancellation_reason in ( 'S3','SC')  and scd_sd.start_date <= scd_sd.bp_start_date and scd_sd.bp_billing_block not in('30', '43', '44') --S4H Updated to include BPBB 43, 44
                                    --and scd_sd.bp_billing_block is not NULL and scd_sd.bp_billing_block <> '' and scd_sd.bp_billing_block <> '30'
                                    and  bill.sales_document is null 
                                        and bill2.sales_document is null 
                                        then 'INACTIVE'                                     
                            -- check on payment transaction status for trial orders
                                when dim_sub.trial_order_flag = 'Y' and coalesce(scd_sd.first_pmt_success_date,'') = '' then 'INACTIVE'
                            -- Check on bill plan bill block and checking for invoices          
                                when dim_sub.subscription_category <> 'SERV' and vbak.AUGRU NOT IN ('911', '912', '913', '921') and scd_sd.bp_billing_block is not NULL and scd_sd.bp_billing_block <> '' and scd_sd.bp_billing_block not in('30', '43', '44') --POSA bill block , S4H change to include BPBB 43,44                             
                                    then case when  bill_doc.sales_document is null 
                            -- Checking the scenario where invoice as there, but link with bp item missing due to bill plan modification                                                    
                                                    then case when  bill_doc2.sales_document is null 
                                                        then 'INACTIVE'
                                                        else 'ACTIVE'
                                                    end
                                                    
                                            else 
                                                'ACTIVE'
                                            end                                                 
                                else
                                    'ACTIVE'
                                end as  subscription_status
                            ,scd_sd.client||scd_sd.sales_document||scd_sd.sales_document_item||to_nvarchar(scd_sd.start_date,'YYYYMMDD') as link_id
                            ,NULL as prev_link_id
                            ,subscription_category
                            ,scd_sd.bp_upd_ind
                        from :TMP_DELTA_SCD_SD_BP scd_sd
                        inner join :dim_sub dim_sub on
                            dim_sub.client = scd_sd.client
                            and dim_sub.sales_document = scd_sd.sales_document
                            and dim_sub.sales_document_item = scd_sd.sales_document_item
                        inner join  REPLICN_S4H.vbak vbak on 
                            scd_sd.client = vbak.mandt 
                            and scd_sd.sales_document = vbak.vbeln
                        left outer join  :bill_docs_g2 credit_memo
                                                    on  credit_memo.sales_document = scd_sd.sales_document
                                                        and credit_memo.sales_document_item = scd_sd.sales_document_item 
                                                        and credit_memo.created_date = scd_sd.start_date
                        left outer join  :bill_docs_s3 bill
                                                    on  bill.sales_document = scd_sd.sales_document
                                                        and    bill.sales_document_item = scd_sd.sales_document_item 
                                                        and    bill.bp_no = scd_sd.bp_no                                                    
                                                        and    bill.bp_item = scd_sd.bp_item
                                                        and    bill.created_date = scd_sd.start_date
                        left outer join  :bill_docs_s3 bill2
                                                                on  bill2.sales_document = scd_sd.sales_document
                                                            and    bill2.sales_document_item = scd_sd.sales_document_item 
                                                            and    bill2.bp_no = scd_sd.bp_no                                                   
                                                            and    bill2.bp_end_date = scd_sd.bp_end_date
                                                            and    bill2.created_date = scd_sd.start_date
                         left outer join :tmp_bill_doc bill_doc on
                                                      bill_doc.sales_document = scd_sd.sales_document
                                                        and    bill_doc.sales_document_item = scd_sd.sales_document_item 
                                                        and    bill_doc.start_date = scd_sd.start_date
                        left outer join :tmp_bill_doc2 bill_doc2
                                                                on  bill_doc2.sales_document = scd_sd.sales_document
                                                            and    bill_doc2.sales_document_item = scd_sd.sales_document_item 
                                                            and    bill_doc2.start_date = scd_sd.start_date                                                                                                 
                ;


end for;

END  
;
