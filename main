create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
AS
set nocount on
begin
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
	declare @ErrorMessage varchar(max)

-- РџСЂРѕРІРµСЂРєР° РЅР° РєРѕСЂСЂРµРєС‚РЅРѕСЃС‚СЊ Р·Р°РіСЂСѓР·РєРё
	if not exists (
	select 1
	from syn.ImportFile as f
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit)
	)
		begin
			set @ErrorMessage = 'РћС€РёР±РєР° РїСЂРё Р·Р°РіСЂСѓР·РєРµ С„Р°Р№Р»Р°, РїСЂРѕРІРµСЂСЊС‚Рµ РєРѕСЂСЂРµРєС‚РЅРѕСЃС‚СЊ РґР°РЅРЅС‹С…'

			raiserror(@ErrorMessage, 3, 1)
			return
		end

	--Р§С‚РµРЅРёРµ РёР· СЃР»РѕСЏ РІСЂРµРјРµРЅРЅС‹С… РґР°РЅРЅС‹С…
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and cd.ID_mapping_DataSource = 1
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- РћРїСЂРµРґРµР»СЏРµРј РЅРµРєРѕСЂСЂРµРєС‚РЅС‹Рµ Р·Р°РїРёСЃРё
	-- Р”РѕР±Р°РІР»СЏРµРј РїСЂРёС‡РёРЅСѓ, РїРѕ РєРѕС‚РѕСЂРѕР№ Р·Р°РїРёСЃСЊ СЃС‡РёС‚Р°РµС‚СЃСЏ РЅРµРєРѕСЂСЂРµРєС‚РЅРѕР№
	select
		cs.*
		,case
			when c.ID is null then 'UID РєР»РёРµРЅС‚Р° РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РІ СЃРїСЂР°РІРѕС‡РЅРёРєРµ "РљР»РёРµРЅС‚"'
			when c_dist.ID is null then 'UID РґРёСЃС‚СЂРёР±СЊСЋС‚РѕСЂР° РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РІ СЃРїСЂР°РІРѕС‡РЅРёРєРµ "РљР»РёРµРЅС‚"'
			when s.ID is null then 'РЎРµР·РѕРЅ РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РІ СЃРїСЂР°РІРѕС‡РЅРёРєРµ "РЎРµР·РѕРЅ"'
			when cst.ID is null then 'РўРёРї РєР»РёРµРЅС‚Р° РѕС‚СЃСѓС‚СЃС‚РІСѓРµС‚ РІ СЃРїСЂР°РІРѕС‡РЅРёРєРµ "РўРёРї РєР»РёРµРЅС‚Р°"'
			when try_cast(cs.DateBegin as date) is null then 'РќРµРІРѕР·РјРѕР¶РЅРѕ РѕРїСЂРµРґРµР»РёС‚СЊ Р”Р°С‚Сѓ РЅР°С‡Р°Р»Р°'
			when try_cast(cs.DateEnd as date) is null then 'РќРµРІРѕР·РјРѕР¶РЅРѕ РѕРїСЂРµРґРµР»РёС‚СЊ Р”Р°С‚Сѓ РѕРєРѕРЅС‡Р°РЅРёСЏ'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'РќРµРІРѕР·РјРѕР¶РЅРѕ РѕРїСЂРµРґРµР»РёС‚СЊ РђРєС‚РёРІРЅРѕСЃС‚СЊ'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where cc.ID is null
		or cd.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- РћР±СЂР°Р±РѕС‚РєР° РґР°РЅРЅС‹С… РёР· С„Р°Р№Р»Р°
	merge into syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched 
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive)
	;

	-- РРЅС„РѕСЂРјР°С†РёРѕРЅРЅРѕРµ СЃРѕРѕР±С‰РµРЅРёРµ
	begin
		select @ErrorMessage = concat('РћР±СЂР°Р±РѕС‚Р°РЅРѕ СЃС‚СЂРѕРє: ', @RowCount)

		raiserror(@ErrorMessage, 1, 1)

		-- Р¤РѕСЂРјРёСЂРѕРІР°РЅРёРµ С‚Р°Р±Р»РёС†С‹ РґР»СЏ РѕС‚С‡РµС‚РЅРѕСЃС‚Рё
		select top 100
			Season as 'РЎРµР·РѕРЅ'
			,UID_DS_Customer as 'UID РљР»РёРµРЅС‚Р°'
			,Customer as 'РљР»РёРµРЅС‚'
			,CustomerSystemType as 'РўРёРї РєР»РёРµРЅС‚Р°'
			,UID_DS_CustomerDistributor as 'UID Р”РёСЃС‚СЂРёР±СЊСЋС‚РѕСЂР°'
			,CustomerDistributor as 'Р”РёСЃС‚СЂРёР±СЊСЋС‚РѕСЂ'
			,isnull(format(try_cast(DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), DateBegin) as 'Р”Р°С‚Р° РЅР°С‡Р°Р»Р°'
			,isnull(format(try_cast(DateEnd as date), 'dd.MM.yyyy', 'ru-RU'), DateEnd) as 'Р”Р°С‚Р° РѕРєРѕРЅС‡Р°РЅРёСЏ'
			,FlagActive as 'РђРєС‚РёРІРЅРѕСЃС‚СЊ'
			,Reason as 'РџСЂРёС‡РёРЅР°'
		from #BadInsertedRows

		return
	end

end
